package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

type admissionSource struct {
	claimed      []string
	released     []string
	transitions  []source.State
	reports      []source.Lifecycle
	events       []string
	resolved     []string
	preLaunchErr error
	preLaunches  []string
}

// failingStatusWriter fails its first patches, then forwards to the fake
// status writer.
type failingStatusWriter struct {
	fakeStatusWriter
	failures int
	calls    int
}

func (w *failingStatusWriter) PatchStatus(ctx context.Context, name types.NamespacedName, patch []byte) error {
	w.calls++
	if w.calls <= w.failures {
		return errors.New("status patch failed")
	}
	return w.fakeStatusWriter.PatchStatus(ctx, name, patch)
}

type fakeWorldObserver struct {
	observation PRObservation
	err         error
	calls       *int
}

func (o fakeWorldObserver) Observe(context.Context, string, string) (PRObservation, error) {
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
	return nil
}
func (s *admissionSource) Report(_ context.Context, _ source.WorkItem, lifecycle source.Lifecycle) error {
	s.reports = append(s.reports, lifecycle)
	s.events = append(s.events, "report:"+string(lifecycle.Result))
	return nil
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
	registry := NewSourceRegistry(map[string]source.Adapter{"first": first, "second": second})
	if got, err := registry.lookup("first"); err != nil || got != first {
		t.Fatalf("lookup(first) = %v, %v; want first adapter", got, err)
	}
	run := admissionRun("run", "local", courierv1alpha1.PhasePending)
	run.Spec.Source = "first"
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
	resolver := ExistingPRHeadResolverFunc(func(context.Context, *courierv1alpha1.CoderRun) (string, error) {
		order = append(order, "resolve")
		return "feature/existing-pr", nil
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
	if updated.Status.Branch != "feature/existing-pr" {
		t.Fatalf("branch = %q, want existing PR head", updated.Status.Branch)
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

func TestFixPRRequiresInjectedHeadResolver(t *testing.T) {
	run := admissionRun("fix", "local", courierv1alpha1.PhasePending)
	run.Spec.Mode = courierv1alpha1.ModeFixPR
	_, err := resolveRunBranch(context.Background(), run, nil)
	if !errors.Is(err, ErrPRHeadResolverRequired) {
		t.Fatalf("resolveRunBranch() error = %v, want resolver-required error", err)
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
		name       string
		exitCode   int32
		wantPhase  courierv1alpha1.Phase
		wantSource source.State
	}{
		{name: "success", exitCode: 0, wantPhase: courierv1alpha1.PhaseVerifying},
		{name: "needs-human", exitCode: 2, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantSource: source.StateNeedsHuman},
		{name: "failure", exitCode: 17, wantPhase: courierv1alpha1.PhaseFailed, wantSource: source.StateNeedsHuman},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseRunning)
			run.Spec.Source = "test"
			run.Spec.WorkItemID = "opaque-work-item"
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
			if calls != 0 {
				t.Fatalf("observer calls = %d, want 0 before Verifying reconcile", calls)
			}

		})
	}
}

func TestVerifyingObservationGatesReview(t *testing.T) {
	tests := []struct {
		name            string
		observation     PRObservation
		err             error
		observerMissing bool
		wantPhase       courierv1alpha1.Phase
		wantRequeue     bool
		wantPR          string
		wantTransition  source.State
	}{
		{name: "observer missing", observerMissing: true, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantTransition: source.StateNeedsHuman},
		{name: "no PR", wantPhase: courierv1alpha1.PhaseNeedsHuman, wantTransition: source.StateNeedsHuman},
		{name: "draft", observation: PRObservation{PR: "42", Draft: true, Checks: []CheckObservation{{State: CheckStatePassed}}}, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantTransition: source.StateNeedsHuman},
		{name: "no checks", observation: PRObservation{PR: "42"}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "pending", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStatePending}}}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "pending failure mix", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}, {State: CheckStatePending}}}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "failed", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}}, wantPhase: courierv1alpha1.PhaseNeedsHuman, wantTransition: source.StateNeedsHuman},
		{name: "single green observation", observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStatePassed}, {State: CheckStatePassed}}}, wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true, wantPR: "42"},
		{name: "transient error", err: errors.New("GitHub unavailable"), wantPhase: courierv1alpha1.PhaseVerifying, wantRequeue: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
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

func TestVerifyingFailureSkipsSettle(t *testing.T) {
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
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman without waiting for a stable observation", updated.Status.Phase)
	}
	if len(item.transitions) != 1 || item.transitions[0] != source.StateNeedsHuman {
		t.Fatalf("transitions = %#v, want exactly one needs-human", item.transitions)
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

func TestTerminalLifecycleReportsToSource(t *testing.T) {
	tests := []struct {
		name        string
		observation PRObservation
		wantPhase   courierv1alpha1.Phase
		wantReport  source.Lifecycle
	}{
		{
			name:        "awaiting review",
			observation: PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "check", State: CheckStatePassed}}},
			wantPhase:   courierv1alpha1.PhaseAwaitingReview,
			wantReport:  source.Lifecycle{State: source.StateInReview, Result: source.ResultReady, PR: "42", IdempotencyKey: "coderun/default/run/AwaitingReview"},
		},
		{
			name:        "needs human",
			observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}},
			wantPhase:   courierv1alpha1.PhaseNeedsHuman,
			wantReport:  source.Lifecycle{State: source.StateNeedsHuman, Result: source.ResultBlocked, PR: "42", Error: "run requires human intervention", IdempotencyKey: "coderun/default/run/NeedsHuman"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &admissionSource{}
			run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
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
			if len(item.reports) != 1 || item.reports[0] != tt.wantReport {
				t.Fatalf("reports = %#v, want %#v", item.reports, []source.Lifecycle{tt.wantReport})
			}
			var updated courierv1alpha1.CoderRun
			if err := client.Get(context.Background(), admissionKey("run"), &updated); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if updated.Status.Phase != tt.wantPhase || updated.Status.PR != "42" {
				t.Fatalf("status = phase %q PR %q, want %q 42", updated.Status.Phase, updated.Status.PR, tt.wantPhase)
			}
		})
	}
}

// TestTerminalLifecycleReportRetryReusesIdempotencyKey checks the retry-safe
// ordering: the lifecycle report precedes the status patch, so a failed patch
// leaves the phase unchanged and the next reconcile re-reports the same
// lifecycle carrying the same idempotency key for the source to deduplicate.
func TestTerminalLifecycleReportRetryReusesIdempotencyKey(t *testing.T) {
	item := &admissionSource{}
	run := admissionRun("run", "local", courierv1alpha1.PhaseVerifying)
	run.Status.Branch = "courier/acme/widgets/issue-1"
	client := phaseClient(t, run)
	writer := &failingStatusWriter{fakeStatusWriter: fakeStatusWriter{client: client}, failures: 1}
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": item}),
		StatusWriter: writer,
		Observer:     fakeWorldObserver{observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}}},
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err == nil {
		t.Fatal("first Reconcile() error = nil, want status patch failure")
	}
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if len(item.reports) != 2 {
		t.Fatalf("reports = %#v, want one per attempt", item.reports)
	}
	want := lifecycleIdempotencyKey(run, courierv1alpha1.PhaseNeedsHuman)
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
	if updated.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase = %q, want NeedsHuman", updated.Status.Phase)
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

const executorRunLabel = "courier.misospace.dev/coderrun"
