package controller

// The mechanical-CI park-and-resume acceptance scenarios (#475, #494).
//
// Every scenario runs against the real API server: the lane's repair policy is
// read from the CRD, the park record and the repair context are persisted in
// the run's status, and the phase changes are the operator's own. The world
// the operator observes is scripted, but nothing else is: no status is written
// by the test except the broker-verified publication record a real harness
// would leave behind, so a scenario proves the operator's behaviour rather
// than the test's.

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

// ciparkPolicy is the repair policy the scenarios declare. The lane names one
// mechanical check, allows two repair attempts, and waits at most half an
// hour for CI.
func ciparkPolicy(maxAttempts int) *courierv1alpha1.CIRepairPolicy {
	return &courierv1alpha1.CIRepairPolicy{
		MechanicalChecks:         []string{"build"},
		MaxAttempts:              maxAttempts,
		ObservationWindowSeconds: 1800,
	}
}

// ciparkWorld is the world the operator observes. It is stateful on purpose:
// a scenario decides what the world looks like at every observation, so a
// repair can republish a new head and a check can move pending → failed →
// passed between two observations.
type ciparkWorld struct {
	observation PRObservation
}

func (w *ciparkWorld) Observe(context.Context, string, HeadRef) (PRObservation, error) {
	return w.observation, nil
}

func (w *ciparkWorld) set(observation PRObservation) {
	w.observation = observation
}

// ciparkHarness drives one run through the park-and-resume loop against the
// real API server.
type ciparkHarness struct {
	t            *testing.T
	name         string
	laneName     string
	adapter      source.Adapter
	world        *ciparkWorld
	launched     []string
	pullRequests *fakePullRequestReader
	reconciler   *CoderRunReconciler
}

// newCIParkHarness creates the lane, the run, and the reconciler that drives
// them. A nil policy leaves the park-and-resume loop disabled, exactly as a
// lane that never declared one does.
func newCIParkHarness(t *testing.T, name string, policy *courierv1alpha1.CIRepairPolicy, adapter source.Adapter) *ciparkHarness {
	t.Helper()
	return newCIParkHarnessMode(t, name, policy, adapter, courierv1alpha1.ModeResolveIssue)
}

// newCIParkHarnessMode creates the same scenario for a run of a chosen mode,
// the spec being immutable after creation.
func newCIParkHarnessMode(t *testing.T, name string, policy *courierv1alpha1.CIRepairPolicy, adapter source.Adapter, mode courierv1alpha1.Mode) *ciparkHarness {
	t.Helper()
	ctx := context.Background()
	laneName := name + "-lane"
	lane := envtestLane(laneName)
	lane.Spec.CIRepair = policy
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	run := envtestRun(name, laneName)
	run.Spec.Mode = mode
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	h := &ciparkHarness{t: t, name: name, laneName: laneName, adapter: adapter, world: &ciparkWorld{}}
	h.reconciler = h.newReconciler()
	return h
}

// newReconciler builds a reconciler over the harness's world and launch
// function. A second instance is how a controller restart is simulated.
func (h *ciparkHarness) newReconciler() *CoderRunReconciler {
	reconciler := &CoderRunReconciler{
		Client: envtestClient,
		Sources: NewSourceRegistry(map[string]source.Adapter{
			"manual": h.adapter,
		}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Observer:     h.world,
		Launch:       h.launch,
	}
	// Only a constructed reader is wired: a nil *fakePullRequestReader field
	// would otherwise enter the interface as a typed nil and defeat the
	// merge observation's own nil guard.
	if h.pullRequests != nil {
		reconciler.PullRequests = h.pullRequests
	}
	return reconciler
}

// usePullRequests gives the harness's reconciler the forge read-pull-request
// capability the post-review merge observation reads through. The reconciler
// is rebuilt the way a deployment wires it, so the capability is present from
// the start of the scenario.
func (h *ciparkHarness) usePullRequests(reader *fakePullRequestReader) {
	h.t.Helper()
	h.pullRequests = reader
	h.reconciler = h.newReconciler()
}

// launch starts the coordinator of one run. A repair relaunches the same run,
// so the pod slot is reused: the previous incarnation's pod carries the
// terminated container status again instead of a second pod existing at once.
func (h *ciparkHarness) launch(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	h.launched = append(h.launched, run.Name)
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
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		var existing corev1.Pod
		if getErr := envtestClient.Get(ctx, client.ObjectKeyFromObject(pod), &existing); getErr != nil {
			return getErr
		}
		pod = &existing
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "coordinator",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
	}}
	return envtestClient.Status().Update(ctx, pod)
}

// reconcile runs one reconcile of the harness's run.
func (h *ciparkHarness) reconcile() {
	h.t.Helper()
	h.reconcileRun(h.name)
}

// reconcileRun runs one reconcile of another run on the harness's lane.
func (h *ciparkHarness) reconcileRun(name string) {
	h.t.Helper()
	h.reconcileResultOf(name)
}

// reconcileResult runs one reconcile of the harness's run and returns its
// result, so a scenario can pin the wake interval the reconcile asks for.
func (h *ciparkHarness) reconcileResult() ctrl.Result {
	h.t.Helper()
	return h.reconcileResultOf(h.name)
}

// reconcileResultOf runs one reconcile of another run on the harness's lane
// and returns its result.
func (h *ciparkHarness) reconcileResultOf(name string) ctrl.Result {
	h.t.Helper()
	result, err := h.reconciler.Reconcile(context.Background(), envtestRequest(name))
	if err != nil {
		h.t.Fatal(err)
	}
	return result
}

// reconcileOutcome runs one reconcile of the harness's run and returns both
// its result and its error, so a scenario can pin the failure the reconcile
// reports as well as the wake interval it asks for.
func (h *ciparkHarness) reconcileOutcome() (ctrl.Result, error) {
	h.t.Helper()
	return h.reconciler.Reconcile(context.Background(), envtestRequest(h.name))
}

// run reads the harness's run back from the API server.
func (h *ciparkHarness) run() *courierv1alpha1.CoderRun {
	h.t.Helper()
	return h.runNamed(h.name)
}

// runExists reports whether the harness's run is still in the API server, so
// a scenario can pin whether the reaper took it away.
func (h *ciparkHarness) runExists() bool {
	h.t.Helper()
	var run courierv1alpha1.CoderRun
	err := envtestClient.Get(context.Background(), envtestKey(h.name), &run)
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	h.t.Fatal(err)
	return false
}

// runNamed reads another run back from the API server.
func (h *ciparkHarness) runNamed(name string) *courierv1alpha1.CoderRun {
	h.t.Helper()
	var run courierv1alpha1.CoderRun
	if err := envtestClient.Get(context.Background(), envtestKey(name), &run); err != nil {
		h.t.Fatal(err)
	}
	return &run
}

// phase reads the harness's run phase.
func (h *ciparkHarness) phase() courierv1alpha1.Phase {
	h.t.Helper()
	return h.run().Status.Phase
}

// publishHead records the head the harness's broker verified as published.
// It is the durable publication evidence the operator attributes a head to a
// run with, and the park never engages without it.
func (h *ciparkHarness) publishHead(sha string) {
	h.t.Helper()
	run := h.run()
	run.Status.LastCommit = sha
	if err := envtestClient.Status().Update(context.Background(), run); err != nil {
		h.t.Fatal(err)
	}
}

// launchCount counts the coordinator launches of one run.
func (h *ciparkHarness) launchCount(name string) int {
	h.t.Helper()
	count := 0
	for _, launched := range h.launched {
		if launched == name {
			count++
		}
	}
	return count
}

// toVerifying brings the run to its published, verifying state: the
// coordinator was launched and exited.
func (h *ciparkHarness) toVerifying() {
	h.t.Helper()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseRunning {
		h.t.Fatalf("phase after the launch = %q, want Running", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		h.t.Fatalf("phase after the coordinator exited = %q, want Verifying", got)
	}
}

// park establishes the park record of the run on its own published head while
// a configured mechanical check is still pending.
func (h *ciparkHarness) park(head string, checks ...CheckObservation) {
	h.t.Helper()
	h.publishHead(head)
	h.world.set(PRObservation{PR: "42", Head: head, Checks: checks})
	h.toVerifying()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		h.t.Fatalf("phase while parked = %q, want Verifying", got)
	}
	parked := h.run()
	if parked.Status.CIPark == nil {
		h.t.Fatal("status.cipark = nil while parked, want the park record")
	}
	if parked.Status.CIPark.Head != head {
		h.t.Fatalf("park head = %q, want %q", parked.Status.CIPark.Head, head)
	}
}

// settleFailure observes the same failed check set twice: the first
// observation records the failure, the second settles it.
func (h *ciparkHarness) settleFailure(head string, names ...string) {
	h.t.Helper()
	h.world.set(redObservation(head, names...))
	h.reconcile()
	h.reconcile()
}

// closeOwedReview drives the harness's run into the shape a terminal run
// carries when it still owes the source a park close: the repair consumed an
// attempt, so the settlement carries the fence the reservation granted, and
// the source's refusal to accept it keeps the close pending on the run.
//
// The run ends in AwaitingReview with the exact pull-request identity it
// published, so a scenario can observe that identity for a merge.
func (h *ciparkHarness) closeOwedReview() {
	h.t.Helper()
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	// The repair wakes the same run for one bounded attempt.
	h.reconcile()
	h.reconcile()
	// The repair republishes a new head, and the repushed outcome is the
	// close the source refuses to accept.
	h.publishHead("sha-2")
	h.world.set(greenObservation("sha-2", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		h.t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	owed := h.run()
	if owed.Status.CISettlement == nil {
		h.t.Fatal("status.ciSettlement = nil, want the pending close the source refuses")
	}
	if owed.Status.CISettlement.FenceToken == "" {
		h.t.Fatal("pending close fence token = empty, want the reserved fence")
	}
	if owed.Status.CIPark == nil || owed.Status.CIPark.Attempts == 0 {
		h.t.Fatalf("park record = %#v, want a consumed attempt the close reports", owed.Status.CIPark)
	}
	// The recorded pull-request identity the merge observation reads by.
	if owed.Status.PR != "42" || owed.Status.Branch != "courier/acme/widgets/issue-14" || owed.Status.HeadRepo != "acme/widgets" {
		h.t.Fatalf("recorded pull-request identity = PR %q branch %q head repo %q, want the identity the observation reads",
			owed.Status.PR, owed.Status.Branch, owed.Status.HeadRepo)
	}
}

// TestEnvtestCIParkRepairsRedMechanicalCI pins the whole repair loop: a red
// mechanical check on the run's own head parks the run, a settled failure
// wakes the same run for one bounded repair, and the republished head that
// goes green finishes the run in review.
func TestEnvtestCIParkRepairsRedMechanicalCI(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-repair-green", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIPark.Attempts != 0 {
		t.Fatalf("park attempts = %d, want 0 before any repair", parked.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 (a parked run holds no coordinator)", got)
	}

	// One failed observation is not a settled failure: the run records it and
	// stays parked, and the coordinator is never woken.
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the first failed observation = %q, want Verifying", got)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 (an unsettled failure never wakes the coordinator)", got)
	}

	// The second identical failure settles it, and the run wakes for exactly
	// one repair.
	branchBefore := h.run().Status.Branch
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase after the settled failure = %q, want Claimed", got)
	}
	resumed := h.run()
	if resumed.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the resume, want the park record")
	}
	if resumed.Status.CIPark.Attempts != 1 {
		t.Fatalf("park attempts = %d, want 1 after the resume", resumed.Status.CIPark.Attempts)
	}
	if resumed.Status.CIRepair == nil {
		t.Fatal("status.ciRepair = nil after the resume, want the repair context")
	}
	if resumed.Status.CIRepair.Head != "sha-1" {
		t.Fatalf("repair head = %q, want %q", resumed.Status.CIRepair.Head, "sha-1")
	}
	if resumed.Status.CIRepair.Attempt != 1 {
		t.Fatalf("repair attempt = %d, want 1", resumed.Status.CIRepair.Attempt)
	}
	if len(resumed.Status.CIRepair.FailedChecks) != 1 || resumed.Status.CIRepair.FailedChecks[0] != "build" {
		t.Fatalf("repair failed checks = %#v, want [build]", resumed.Status.CIRepair.FailedChecks)
	}
	if resumed.Status.Branch != branchBefore {
		t.Fatalf("branch after the resume = %q, want %q (a resume never re-resolves the branch)", resumed.Status.Branch, branchBefore)
	}

	// The resumed coordinator runs and exits.
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase after the resumed launch = %q, want Running", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the resumed coordinator exited = %q, want Verifying", got)
	}
	if got := h.launchCount(h.name); got != 2 {
		t.Fatalf("launches = %d, want 2 (the initial launch and one repair)", got)
	}

	// The repair republished a new head, and its mechanical check is green.
	h.publishHead("sha-2")
	h.world.set(greenObservation("sha-2", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the first green observation = %q, want Verifying", got)
	}
	// The repair context of the finished attempt is cleared once the attempt
	// is observed.
	if got := h.run().Status.CIRepair; got != nil {
		t.Fatalf("status.ciRepair = %#v after the attempt finished, want nil", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the repair, want the park record")
	}
	if final.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeRepushed {
		t.Fatalf("park outcome = %q, want %q", final.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeRepushed)
	}
	if final.Status.CIPark.Attempts != 1 {
		t.Fatalf("park attempts = %d, want 1", final.Status.CIPark.Attempts)
	}
	if final.Status.CIPark.Head != "sha-2" {
		t.Fatalf("park head = %q, want the republished head sha-2", final.Status.CIPark.Head)
	}
}

// TestEnvtestCIParkGreenFinishesWithoutRepair pins the cheapest ending: a
// green mechanical check settles the run without ever waking the coordinator.
func TestEnvtestCIParkGreenFinishesWithoutRepair(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-green", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the first green observation = %q, want Verifying", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the green settle, want the park record")
	}
	if final.Status.CIPark.Attempts != 0 {
		t.Fatalf("park attempts = %d, want 0 (a green run never repairs)", final.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 (a green run never wakes the coordinator)", got)
	}
}

// TestEnvtestCIParkWithoutEligibleChecksHandsOff pins the run that reports no
// check the lane configures: it hands off instead of parking forever.
func TestEnvtestCIParkWithoutEligibleChecksHandsOff(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-no-eligible", ciparkPolicy(2), &admissionSource{})

	h.publishHead("sha-1")
	h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "ai-review", State: CheckStatePassed}}})
	h.toVerifying()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the ineligible check set = %q, want HandedOff", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the handoff, want the park record")
	}
	if final.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("park outcome = %q, want %q", final.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeUnsupported)
	}
	if final.Status.CIPark.Attempts != 0 {
		t.Fatalf("park attempts = %d, want 0", final.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 (an ineligible check set never wakes the coordinator)", got)
	}
}

// TestEnvtestCIParkPartialRegistrationNeverSettlesGreen pins the coverage
// rule: a lane configuring two mechanical checks whose observation carries
// only one of them passing is not mechanically green, even observed twice.
// The run hands off instead of settling onto review with a check that never
// registered.
func TestEnvtestCIParkPartialRegistrationNeverSettlesGreen(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	policy := ciparkPolicy(2)
	policy.MechanicalChecks = []string{"build", "test"}
	h := newCIParkHarness(t, "cipark-partial-green", policy, &admissionSource{})

	h.publishHead("sha-1")
	h.world.set(greenObservation("sha-1", "build"))
	h.toVerifying()
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the partial green observation = %q, want HandedOff", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the handoff, want the park record")
	}
	if final.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("park outcome = %q, want %q", final.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeUnsupported)
	}
	if final.Status.CIPark.Attempts != 0 {
		t.Fatalf("park attempts = %d, want 0 (a partial check set never repairs)", final.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 (a partial check set never wakes the coordinator)", got)
	}
}

// TestEnvtestCIParkExhaustedBudgetHandsOff pins the budget boundary: with one
// repair allowed, the second failure of the same head ends the run instead of
// waking the coordinator again.
func TestEnvtestCIParkExhaustedBudgetHandsOff(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-exhausted", ciparkPolicy(1), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase after the settled failure = %q, want Claimed", got)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 before the resume", got)
	}

	// The repaired coordinator runs and exits, and the head still fails.
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase after the resumed launch = %q, want Running", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the resumed coordinator exited = %q, want Verifying", got)
	}
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the exhausted budget = %q, want HandedOff", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the handoff, want the park record")
	}
	if final.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeExhausted {
		t.Fatalf("park outcome = %q, want %q", final.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeExhausted)
	}
	if final.Status.CIPark.Attempts != 1 {
		t.Fatalf("park attempts = %d, want 1", final.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 2 {
		t.Fatalf("launches = %d, want 2 (the initial launch and one repair, never a second repair)", got)
	}
}

// TestEnvtestCIParkNoProgressHandsOff pins the no-progress boundary: a repair
// attempt that publishes nothing ends the run, even though the budget still
// had room for another attempt.
func TestEnvtestCIParkNoProgressHandsOff(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-no-progress", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase after the settled failure = %q, want Claimed", got)
	}

	// The repaired coordinator runs and exits without publishing anything:
	// the head does not move and the recorded commit stays the same.
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase after the resumed launch = %q, want Running", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the resumed coordinator exited = %q, want Verifying", got)
	}
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the repair published nothing = %q, want HandedOff", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the handoff, want the park record")
	}
	if final.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeExhausted {
		t.Fatalf("park outcome = %q, want %q", final.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeExhausted)
	}
	if final.Status.CIPark.Attempts != 1 {
		t.Fatalf("park attempts = %d, want 1 (a repair that publishes nothing spends no further attempt)", final.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 2 {
		t.Fatalf("launches = %d, want 2 (the initial launch and one repair, never a second repair)", got)
	}
}

// TestEnvtestCIParkNonMechanicalChecksNeverWake pins the trigger separation:
// a check the lane never named can neither wake the coordinator nor hold the
// park open.
func TestEnvtestCIParkNonMechanicalChecksNeverWake(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}

	t.Run("a pending ai-review never extends the park", func(t *testing.T) {
		h := newCIParkHarness(t, "cipark-ai-pending", ciparkPolicy(2), &admissionSource{})
		h.publishHead("sha-1")
		h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "ai-review", State: CheckStatePending}}})
		h.toVerifying()
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after the pending ai-review = %q, want HandedOff", got)
		}
		final := h.run()
		if final.Status.CIPark.Attempts != 0 {
			t.Fatalf("park attempts = %d, want 0", final.Status.CIPark.Attempts)
		}
		if got := h.launchCount(h.name); got != 1 {
			t.Fatalf("launches = %d, want 1 (a pending ai-review never wakes the coordinator)", got)
		}
	})

	t.Run("a failed ai-review never wakes the coordinator", func(t *testing.T) {
		h := newCIParkHarness(t, "cipark-ai-failed", ciparkPolicy(2), &admissionSource{})
		h.publishHead("sha-1")
		h.world.set(redObservation("sha-1", "ai-review"))
		h.toVerifying()
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the first failed ai-review = %q, want Verifying", got)
		}
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after the settled failed ai-review = %q, want HandedOff", got)
		}
		final := h.run()
		if final.Status.CIPark.Attempts != 0 {
			t.Fatalf("park attempts = %d, want 0", final.Status.CIPark.Attempts)
		}
		if got := h.launchCount(h.name); got != 1 {
			t.Fatalf("launches = %d, want 1 (a failed ai-review never wakes the coordinator)", got)
		}
	})
}

// TestEnvtestCIParkForeignHeadFailsClosed pins the ownership boundary: a head
// the run cannot account for is somebody else's work, and the run hands off
// instead of repairing it.
func TestEnvtestCIParkForeignHeadFailsClosed(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-foreign", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.world.set(PRObservation{PR: "42", Head: "sha-foreign", Checks: []CheckObservation{{Name: "build", State: CheckStateFailed}}})
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the foreign head = %q, want HandedOff", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the handoff, want the park record")
	}
	// A park record carrying the unsupported outcome is the durable account
	// here; the inapplicability condition is never set alongside it.
	if cond := apimeta.FindStatusCondition(final.Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable); cond != nil {
		t.Fatalf("condition = %#v, want none when a park record already accounts for the foreign head", cond)
	}
	if final.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("park outcome = %q, want %q", final.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeUnsupported)
	}
	if final.Status.CIPark.Attempts != 0 {
		t.Fatalf("park attempts = %d, want 0", final.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 (a foreign head never wakes the coordinator)", got)
	}
}

// TestEnvtestCIParkSupersededHeadFailsClosed pins the ownership boundary a
// stale observation crosses: a completed brief's commit stays in the run's
// durable account, but once the broker-verified publication record names a
// newer head, an observation of the older one is superseded — the run hands
// off instead of resuming a repair of a head its own account has moved past.
func TestEnvtestCIParkSupersededHeadFailsClosed(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-superseded", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})

	// The completed brief's durable record carries the parked head, exactly
	// as the harness leaves it alongside the confirmed published commit.
	withCompletedBrief(t, h, "sha-1")

	// The run publishes a newer head: the broker-verified account now names
	// sha-2, which supersedes every older commit the run accounts for.
	h.publishHead("sha-2")

	// A stale observation of the superseded head reports its mechanical
	// check red. It must fail closed, not resume a repair of a head the
	// run's own publication record has moved past.
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the superseded head = %q, want HandedOff", got)
	}
	final := h.run()
	if final.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the handoff, want the park record")
	}
	if final.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("park outcome = %q, want %q", final.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeUnsupported)
	}
	if final.Status.CIPark.Attempts != 0 {
		t.Fatalf("park attempts = %d, want 0 (a superseded head never repairs)", final.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches = %d, want 1 (a superseded head never wakes the coordinator)", got)
	}
}

// withCompletedBrief records a completed brief carrying the given commit in
// the harness run's durable checkpoint, the way CheckpointPublished leaves it.
func withCompletedBrief(t *testing.T, h *ciparkHarness, commit string) {
	t.Helper()
	run := h.run()
	run.Status.Checkpoint = &courierv1alpha1.Checkpoint{
		CompletedBriefs: []courierv1alpha1.Brief{{ID: "brief-1", Commit: commit}},
	}
	if err := envtestClient.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
}

// TestEnvtestCIParkHoldsNoLaneCapacity pins the capacity boundary: a parked
// run holds no lane slot, so the lane still admits another run.
func TestEnvtestCIParkHoldsNoLaneCapacity(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	h := newCIParkHarness(t, "cipark-capacity", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase while parked = %q, want Verifying", got)
	}

	second := envtestRun("cipark-capacity-second", h.laneName)
	if err := envtestClient.Create(ctx, second); err != nil {
		t.Fatal(err)
	}
	h.reconcileRun(second.Name)
	admitted := h.runNamed(second.Name)
	if admitted.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase of the second run = %q, want Running (the parked run holds no lane slot)", admitted.Status.Phase)
	}
}

// TestEnvtestCIParkSurvivesControllerRestart pins the restart boundary: the
// park record is durable, and a restart during the park never consumes a
// second repair attempt.
func TestEnvtestCIParkSurvivesControllerRestart(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-restart", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})

	// A controller restart: a fresh reconciler picks the parked run up
	// again and the park record survives it.
	h.reconciler = h.newReconciler()
	h.reconcile()
	parked := h.run()
	if parked.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the restart, want the park record")
	}
	if parked.Status.CIPark.Head != "sha-1" {
		t.Fatalf("park head = %q, want %q after the restart", parked.Status.CIPark.Head, "sha-1")
	}
	if parked.Status.CIPark.Attempts != 0 {
		t.Fatalf("park attempts = %d, want 0 after the restart", parked.Status.CIPark.Attempts)
	}

	h.settleFailure("sha-1", "build")
	if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase after the settled failure = %q, want Claimed", got)
	}
	resumed := h.run()
	if resumed.Status.CIPark.Attempts != 1 {
		t.Fatalf("park attempts = %d, want exactly 1 after the resume", resumed.Status.CIPark.Attempts)
	}

	// Further reconciles of the resumed run consume no further attempt, even
	// across a restart right after the resume: the attempt was consumed in
	// the same patch as the phase change.
	h.reconciler = h.newReconciler()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase after the resumed launch = %q, want Running", got)
	}
	h.reconcile()
	settled := h.run()
	if settled.Status.CIPark.Attempts != 1 {
		t.Fatalf("park attempts = %d, want exactly 1 (a restart never double-consumes an attempt)", settled.Status.CIPark.Attempts)
	}
	if got := h.launchCount(h.name); got != 2 {
		t.Fatalf("launches = %d, want 2 (the initial launch and one repair)", got)
	}
}

// ciparkSource is a source adapter that implements the optional
// source.CIParkLifecycle capability and records every call the operator makes
// through it. The embedded admissionSource provides the ordinary adapter
// surface, so the two paths differ only in the capability.
type ciparkSource struct {
	admissionSource
	parks        []source.CIParkNotice
	items        []source.WorkItem
	reservations []source.CIRepairReservation
	outcomes     []source.CIRepairOutcome
	releases     []source.CIParkRelease
}

func (s *ciparkSource) ReportCIParked(_ context.Context, item source.WorkItem, notice source.CIParkNotice) error {
	s.items = append(s.items, item)
	s.parks = append(s.parks, notice)
	return nil
}

func (s *ciparkSource) ReserveCIRepair(_ context.Context, item source.WorkItem, reservation source.CIRepairReservation) (source.CIRepairGrant, error) {
	s.items = append(s.items, item)
	s.reservations = append(s.reservations, reservation)
	return source.CIRepairGrant{FenceToken: reservation.FenceToken}, nil
}

func (s *ciparkSource) ReportCIRepair(_ context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	s.items = append(s.items, item)
	s.outcomes = append(s.outcomes, outcome)
	return nil
}

func (s *ciparkSource) ReleaseCIPark(_ context.Context, item source.WorkItem, release source.CIParkRelease) error {
	s.items = append(s.items, item)
	s.releases = append(s.releases, release)
	return nil
}

// staleReleaseSource models a source whose deferral is already gone: the
// release resolves with the advisory refusal the close's vocabulary calls
// "no deferral exists". A refusal is not a delivery, so nothing is recorded.
type staleReleaseSource struct {
	ciparkSource
}

func (s *staleReleaseSource) ReleaseCIPark(ctx context.Context, item source.WorkItem, release source.CIParkRelease) error {
	s.items = append(s.items, item)
	return source.ErrStaleWork
}

// supersededReleaseSource models a source whose deferral is not this run's
// to close: the release resolves with ErrSuperseded — a newer generation or
// a granted reservation owns the deferral, and its own lifecycle releases
// it. A refusal is not a delivery, so nothing is recorded.
type supersededReleaseSource struct {
	ciparkSource
}

func (s *supersededReleaseSource) ReleaseCIPark(ctx context.Context, item source.WorkItem, release source.CIParkRelease) error {
	s.items = append(s.items, item)
	return source.ErrSuperseded
}

// flakyReleaseSource models one lost close response for the release path:
// the source commits the close and loses the response, so the re-presented
// release must be byte-identical — the same key collapsing into the one
// committed close. The default mode is a single loss; every close the source
// saw is recorded.
type flakyReleaseSource struct {
	ciparkSource
	firstCloseLost bool
}

func (s *flakyReleaseSource) ReleaseCIPark(ctx context.Context, item source.WorkItem, release source.CIParkRelease) error {
	s.items = append(s.items, item)
	if !s.firstCloseLost {
		s.firstCloseLost = true
		s.releases = append(s.releases, release)
		return errors.New("dispatch server: response lost")
	}
	s.releases = append(s.releases, release)
	return nil
}

// unsupportedParkSource models a source whose park capability cannot defer
// this run's work at all: the reserve reports source.ErrUnsupportedCIPark,
// the sentinel a Dispatch adapter returns for implement and queue-less items.
type unsupportedParkSource struct {
	ciparkSource
}

func (s *unsupportedParkSource) ReserveCIRepair(ctx context.Context, item source.WorkItem, reservation source.CIRepairReservation) (source.CIRepairGrant, error) {
	s.items = append(s.items, item)
	s.reservations = append(s.reservations, reservation)
	return source.CIRepairGrant{}, source.ErrUnsupportedCIPark
}

// latchedParkSource models a source whose adapter has latched the CI park
// lifecycle as unsupported — the permanent, per-process observation a
// Dispatch deployment earns by answering a park-family route with a bare
// 404 (#559). While the latch is unset the source behaves exactly like
// ciparkSource; once set, every lifecycle exchange reports
// source.ErrUnsupportedCIPark, and the probe reports the latch to the core.
type latchedParkSource struct {
	ciparkSource
	unsupported bool
}

func (s *latchedParkSource) CIParkLifecycleUnsupported() bool {
	return s.unsupported
}

func (s *latchedParkSource) ReportCIParked(ctx context.Context, item source.WorkItem, notice source.CIParkNotice) error {
	if s.unsupported {
		return source.ErrUnsupportedCIPark
	}
	return s.ciparkSource.ReportCIParked(ctx, item, notice)
}

func (s *latchedParkSource) ReserveCIRepair(ctx context.Context, item source.WorkItem, reservation source.CIRepairReservation) (source.CIRepairGrant, error) {
	if s.unsupported {
		s.items = append(s.items, item)
		s.reservations = append(s.reservations, reservation)
		return source.CIRepairGrant{}, source.ErrUnsupportedCIPark
	}
	return s.ciparkSource.ReserveCIRepair(ctx, item, reservation)
}

func (s *latchedParkSource) ReportCIRepair(ctx context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	if s.unsupported {
		return source.ErrUnsupportedCIPark
	}
	return s.ciparkSource.ReportCIRepair(ctx, item, outcome)
}

func (s *latchedParkSource) ReleaseCIPark(ctx context.Context, item source.WorkItem, release source.CIParkRelease) error {
	if s.unsupported {
		s.items = append(s.items, item)
		s.releases = append(s.releases, release)
		return source.ErrUnsupportedCIPark
	}
	return s.ciparkSource.ReleaseCIPark(ctx, item, release)
}

// TestEnvtestCIParkSourceCapability pins the optional source capability: a
// source that implements it is told about the park, the reservation, and the
// outcome, and a source that does not implement it still completes the run.
func TestEnvtestCIParkSourceCapability(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}

	t.Run("a source without the capability still completes", func(t *testing.T) {
		h := newCIParkHarness(t, "cipark-plain-source", ciparkPolicy(2), &admissionSource{})
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		h.world.set(greenObservation("sha-1", "build"))
		h.reconcile()
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
			t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
		}
		final := h.run()
		if final.Status.CIPark.Attempts != 0 {
			t.Fatalf("park attempts = %d, want 0", final.Status.CIPark.Attempts)
		}
	})

	t.Run("a source with the capability is used", func(t *testing.T) {
		adapter := &ciparkSource{}
		h := newCIParkHarness(t, "cipark-capable-source", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		h.settleFailure("sha-1", "build")
		if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
			t.Fatalf("phase after the settled failure = %q, want Claimed", got)
		}
		if len(adapter.parks) != 1 {
			t.Fatalf("park notices = %#v, want exactly one", adapter.parks)
		}
		if adapter.parks[0].PR != "42" {
			t.Fatalf("park notice PR = %q, want 42", adapter.parks[0].PR)
		}
		if adapter.parks[0].Head != "sha-1" {
			t.Fatalf("park notice head = %q, want sha-1", adapter.parks[0].Head)
		}
		if adapter.parks[0].IdempotencyKey == "" {
			t.Fatal("park notice idempotency key = empty, want a stable key")
		}
		if len(adapter.reservations) != 1 {
			t.Fatalf("repair reservations = %#v, want exactly one", adapter.reservations)
		}
		reservation := adapter.reservations[0]
		if reservation.PR != "42" {
			t.Fatalf("reservation PR = %q, want 42", reservation.PR)
		}
		if reservation.Head != "sha-1" {
			t.Fatalf("reservation head = %q, want sha-1", reservation.Head)
		}
		if reservation.FenceToken == "" {
			t.Fatal("reservation fence token = empty, want the operator's fence")
		}
		if reservation.IdempotencyKey == "" {
			t.Fatal("reservation idempotency key = empty, want a stable key")
		}
		if len(adapter.items) == 0 || adapter.items[0].ID != h.name {
			t.Fatalf("capability work items = %#v, want the run's own work item", adapter.items)
		}

		// The repair republishes a new head, and the operator reports the
		// fenced outcome of that attempt to the source.
		h.reconcile()
		h.reconcile()
		h.publishHead("sha-2")
		h.world.set(greenObservation("sha-2", "build"))
		h.reconcile()
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
			t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
		}
		if len(adapter.outcomes) != 1 {
			t.Fatalf("repair outcomes = %#v, want exactly one", adapter.outcomes)
		}
		outcome := adapter.outcomes[0]
		if outcome.FenceToken != reservation.FenceToken {
			t.Fatalf("outcome fence token = %q, want the reserved token %q", outcome.FenceToken, reservation.FenceToken)
		}
		if outcome.PR != "42" {
			t.Fatalf("outcome PR = %q, want 42", outcome.PR)
		}
		if outcome.Head != "sha-2" {
			t.Fatalf("outcome head = %q, want the republished head sha-2", outcome.Head)
		}
		if outcome.Result != source.CIRepairRepushed {
			t.Fatalf("outcome result = %q, want %q", outcome.Result, source.CIRepairRepushed)
		}
	})

	t.Run("a source that cannot defer the run finishes it", func(t *testing.T) {
		adapter := &unsupportedParkSource{}
		h := newCIParkHarness(t, "cipark-unsupported-reserve", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		h.settleFailure("sha-1", "build")
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after the unsupported reservation = %q, want HandedOff", got)
		}
		parked := h.run()
		if parked.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
			t.Fatalf("park outcome = %q, want %q", parked.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeUnsupported)
		}
		if len(adapter.reservations) != 1 {
			t.Fatalf("reservations = %d, want exactly one (an unsupported reserve finishes the run, never retries)", len(adapter.reservations))
		}
	})
}

// TestEnvtestCIParkLatchedUnsupportedTakesTheOrdinaryPath pins the safe
// degradation (#559): a source whose adapter has latched the CI park
// lifecycle as unsupported — permanent version-skew evidence for the
// process — never establishes a park again. A fresh run with a repair policy
// takes the ordinary post-publication grading path (no park record, no
// deferral, no reservation, no burned attempt), a park already in flight
// when the latch flips still finishes fail-closed without consuming an
// attempt, and a probe that reports not-known-unsupported parks exactly as
// today.
func TestEnvtestCIParkLatchedUnsupportedTakesTheOrdinaryPath(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}

	t.Run("a fresh run never establishes a park on a latched source", func(t *testing.T) {
		adapter := &latchedParkSource{unsupported: true}
		h := newCIParkHarness(t, "cipark-probe-latched-fresh", ciparkPolicy(2), adapter)
		h.publishHead("sha-1")
		h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
		h.toVerifying()
		h.reconcile()
		run := h.run()
		if run.Status.CIPark != nil {
			t.Fatalf("status.cipark = %#v on a latched source, want no park record", run.Status.CIPark)
		}
		if run.Status.CIParkDeferral != nil {
			t.Fatalf("status.ciparkDeferral = %#v on a latched source, want no deferral record", run.Status.CIParkDeferral)
		}
		if len(adapter.parks) != 0 {
			t.Fatalf("park notices = %d, want 0", len(adapter.parks))
		}
		// The ordinary path decides: a red check hands the run off without
		// waking a coordinator or spending a repair attempt.
		h.world.set(redObservation("sha-1", "build"))
		h.reconcile()
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after a red check on a latched source = %q, want HandedOff", got)
		}
		if got := h.launchCount(h.name); got != 1 {
			t.Fatalf("launch count = %d, want 1: the ordinary path never wakes a coordinator", got)
		}
		if len(adapter.reservations) != 0 {
			t.Fatalf("reservations = %d, want 0: the latch never burns a repair attempt", len(adapter.reservations))
		}
		if run := h.run(); run.Status.CIPark != nil {
			t.Fatalf("status.cipark = %#v, want no park record", run.Status.CIPark)
		}
	})

	t.Run("a park in flight finishes unsupported without an attempt", func(t *testing.T) {
		adapter := &latchedParkSource{}
		h := newCIParkHarness(t, "cipark-probe-latched-inflight", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		if len(adapter.parks) != 1 {
			t.Fatalf("park notices = %d, want exactly one while the source is not latched", len(adapter.parks))
		}
		// The latch flips mid-flight, the way a first bare 404 lands: the
		// in-flight park keeps today's fail-closed behavior.
		adapter.unsupported = true
		h.settleFailure("sha-1", "build")
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after the unsupported reservation = %q, want HandedOff", got)
		}
		parked := h.run()
		if parked.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
			t.Fatalf("park outcome = %q, want %q", parked.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeUnsupported)
		}
		if parked.Status.CIPark.Attempts != 0 {
			t.Fatalf("park attempts = %d, want 0: the latch never consumes an attempt", parked.Status.CIPark.Attempts)
		}
		if got := h.launchCount(h.name); got != 1 {
			t.Fatalf("launch count = %d, want 1: the sentinel never wakes the coordinator", got)
		}
		if len(adapter.reservations) != 1 {
			t.Fatalf("reservations = %d, want exactly one (the refused reserve, never retried)", len(adapter.reservations))
		}
	})

	t.Run("a probe that reports not-known-unsupported parks as today", func(t *testing.T) {
		adapter := &latchedParkSource{}
		h := newCIParkHarness(t, "cipark-probe-unknown", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		h.settleFailure("sha-1", "build")
		if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
			t.Fatalf("phase after the settled failure = %q, want Claimed", got)
		}
		if len(adapter.parks) != 1 || len(adapter.reservations) != 1 {
			t.Fatalf("park notices = %d reservations = %d, want one of each", len(adapter.parks), len(adapter.reservations))
		}
	})
}

// flakySettlementSource models one lost report for the park notice and one
// for the settlement. The default mode commits the report at the source and
// loses only its response, so a replay must collapse into the committed
// transition; lostBeforeCommit makes the first report of its kind fail
// before the source records anything, so the replay is the first successful
// delivery. Every report the source saw is recorded.
type flakySettlementSource struct {
	ciparkSource
	noticeLostBeforeCommit     bool
	settlementLostBeforeCommit bool
	firstNoticeLost            bool
	firstReportLost            bool
}

func (s *flakySettlementSource) ReportCIParked(ctx context.Context, item source.WorkItem, notice source.CIParkNotice) error {
	s.items = append(s.items, item)
	if !s.firstNoticeLost {
		s.firstNoticeLost = true
		if !s.noticeLostBeforeCommit {
			s.parks = append(s.parks, notice)
		}
		return errors.New("dispatch server: response lost")
	}
	s.parks = append(s.parks, notice)
	return nil
}

func (s *flakySettlementSource) ReportCIRepair(ctx context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	s.items = append(s.items, item)
	if !s.firstReportLost {
		s.firstReportLost = true
		if !s.settlementLostBeforeCommit {
			s.outcomes = append(s.outcomes, outcome)
		}
		return errors.New("dispatch server: response lost")
	}
	s.outcomes = append(s.outcomes, outcome)
	return nil
}

// refusingSettlementSource models a source that refuses the first settlement
// report outright: the outcome is not committed and the record stays pending
// on the run. That is the state a settlement record is in when the park
// account it derives its key from is about to disappear — the record outlives
// the account, and every later report is accepted.
type refusingSettlementSource struct {
	ciparkSource
	firstReportRefused bool
}

func (s *refusingSettlementSource) ReportCIRepair(_ context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	s.items = append(s.items, item)
	if !s.firstReportRefused {
		s.firstReportRefused = true
		return errors.New("dispatch server: report refused")
	}
	s.outcomes = append(s.outcomes, outcome)
	return nil
}

// TestEnvtestCIParkLostSettlementReplays pins the durable settlement: a
// repair outcome whose report is lost — with the response lost after the
// source committed it, or the report lost before it committed — leaves
// durable retry intent on the run (the fence travels with the record), a
// later reconcile re-presents it after a controller restart, and the source
// ends up with the outcome delivered exactly once with the fence it issued.
func TestEnvtestCIParkLostSettlementReplays(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	for name, lostBeforeCommit := range map[string]bool{
		"response lost after commit": false,
		"failure before commit":      true,
	} {
		lostBeforeCommit := lostBeforeCommit
		t.Run(name, func(t *testing.T) {
			adapter := &flakySettlementSource{settlementLostBeforeCommit: lostBeforeCommit}
			h := newCIParkHarness(t, "cipark-lost-settlement-"+strings.ReplaceAll(name, " ", "-"), ciparkPolicy(2), adapter)
			h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
			h.settleFailure("sha-1", "build")
			h.reconcile()
			h.reconcile()
			// The repair publishes nothing: the same red head is a settled
			// failure with no progress, so the attempt ends as exhausted and
			// the settlement carries the fence the reservation granted.
			h.world.set(redObservation("sha-1", "build"))
			h.reconcile()
			if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
				t.Fatalf("phase after the repair published nothing = %q, want HandedOff", got)
			}
			ended := h.run()
			if ended.Status.CISettlement == nil {
				t.Fatal("status.ciSettlement = nil after the lost report, want the pending settlement")
			}
			if ended.Status.CISettlement.Result != courierv1alpha1.CIParkOutcomeExhausted {
				t.Fatalf("pending settlement result = %q, want exhausted", ended.Status.CISettlement.Result)
			}
			if ended.Status.CISettlement.FenceToken == "" {
				t.Fatal("pending settlement fence token = empty, want the reserved fence")
			}
			// The retry re-presents the same settlement; the source accepts it
			// once. A controller restart in between changes nothing: the
			// pending settlement is durable intent on the run, and a fresh
			// reconciler delivers it.
			h.reconciler = h.newReconciler()
			h.reconcile()
			if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
				t.Fatalf("phase after the settlement replay = %q, want HandedOff", got)
			}
			delivered := h.run()
			if delivered.Status.CISettlement != nil {
				t.Fatalf("status.ciSettlement = %#v after the replay, want nil (the clear is the proof of delivery)", delivered.Status.CISettlement)
			}
			wantOutcomes := 2
			if lostBeforeCommit {
				wantOutcomes = 1
			}
			if len(adapter.outcomes) != wantOutcomes {
				t.Fatalf("source outcomes = %#v, want %d", adapter.outcomes, wantOutcomes)
			}
			if !lostBeforeCommit && !reflect.DeepEqual(adapter.outcomes[0], adapter.outcomes[1]) {
				t.Fatalf("replayed outcome = %#v, want byte-identical to the committed one %#v (the same idempotency key collapses into one transition)", adapter.outcomes[1], adapter.outcomes[0])
			}
			outcome := adapter.outcomes[len(adapter.outcomes)-1]
			if outcome.Result != source.CIRepairExhausted {
				t.Fatalf("delivered outcome result = %q, want exhausted", outcome.Result)
			}
			if outcome.FenceToken == "" || outcome.IdempotencyKey == "" {
				t.Fatalf("delivered outcome = %#v, want the fence and the stable idempotency key", outcome)
			}
			// The settled run stays settled: further reconciles spend no
			// further attempt and re-deliver nothing.
			h.reconcile()
			settled := h.run()
			if settled.Status.CIPark.Attempts != 1 {
				t.Fatalf("park attempts = %d, want 1 (a settlement replay never wakes a coordinator)", settled.Status.CIPark.Attempts)
			}
			if len(adapter.outcomes) != wantOutcomes {
				t.Fatalf("source outcomes = %d, want %d after a further reconcile", len(adapter.outcomes), wantOutcomes)
			}
		})
	}
}

// TestEnvtestCIParkRollbackPreservesRepairContext pins #547's rollback
// posture: a repair-resumed run whose launch fails transiently keeps the
// repair context intact through the rollback to Pending — the attempt is
// deferred, not over. Neither the failed launch (releaseClaim) nor a failed
// pre-launch guard (rejectClaim) reports anything to the source or records a
// settlement: the reservation stays held for the outcome the park loop will
// eventually present. The re-admitted run launches on the same fence with
// the same context, re-derives the branch identity the rollback cleared, and
// when its session publishes nothing the preserved attempt fires the
// no-progress guard: the run hands off exhausted with red CI instead of
// reaching review, and the delivered outcome carries the one fence the
// source granted.
func TestEnvtestCIParkRollbackPreservesRepairContext(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarness(t, "cipark-rollback-preserve", ciparkPolicy(2), adapter)

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase after the settled failure = %q, want Claimed", got)
	}
	resumed := h.run()
	if resumed.Status.CIRepair == nil {
		t.Fatal("status.ciRepair = nil after the resume, want the repair context")
	}
	if len(adapter.reservations) != 1 {
		t.Fatalf("repair reservations = %#v, want exactly one", adapter.reservations)
	}
	granted := resumed.Status.CIRepair.FenceToken
	if granted == "" {
		t.Fatal("repair fence token = empty, want the source-granted fence")
	}
	wantContext := resumed.Status.CIRepair.DeepCopy()
	branchBefore := resumed.Status.Branch

	// The launch of the resumed coordinator fails transiently, and the claim
	// rolls back to Pending. The repair context survives intact: it carries
	// the repair goal, the evidence, and the fence of an attempt the operator
	// intends to resume. The branch identity is still rolled back, because
	// re-admission re-derives it.
	wantErr := errors.New("cipark envtest launch failed")
	h.reconciler.Launch = func(context.Context, *courierv1alpha1.CoderRun) error { return wantErr }
	if _, err := h.reconcileOutcome(); !errors.Is(err, wantErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, wantErr)
	}
	rolledBack := h.run()
	if got := rolledBack.Status.Phase; got != courierv1alpha1.PhasePending {
		t.Fatalf("phase after the failed launch = %q, want Pending", got)
	}
	if !reflect.DeepEqual(rolledBack.Status.CIRepair, wantContext) {
		t.Fatalf("status.ciRepair = %#v after the rollback, want the attempt context %#v intact", rolledBack.Status.CIRepair, wantContext)
	}
	if rolledBack.Status.CISettlement != nil {
		t.Fatalf("status.ciSettlement = %#v after a transient rollback, want nil (a deferred attempt owes no outcome yet)", rolledBack.Status.CISettlement)
	}
	if rolledBack.Status.Branch != "" || rolledBack.Status.HeadRepo != "" || rolledBack.Status.HeadSHA != "" {
		t.Fatalf("branch identity after the rollback = %q/%q/%q, want cleared for re-derivation",
			rolledBack.Status.Branch, rolledBack.Status.HeadRepo, rolledBack.Status.HeadSHA)
	}
	if rolledBack.Status.CIPark == nil || rolledBack.Status.CIPark.Head != "sha-1" || rolledBack.Status.CIPark.Attempts != 1 {
		t.Fatalf("park record after the rollback = %#v, want the sha-1 park with its consumed attempt", rolledBack.Status.CIPark)
	}
	if len(adapter.outcomes) != 0 {
		t.Fatalf("source outcomes = %#v, want none (a deferred attempt reports nothing)", adapter.outcomes)
	}

	// The pre-launch guard's transient refusal rolls back the same way: the
	// re-admission that reaches it must not drop the context either.
	h.reconciler.Launch = h.launch
	preErr := errors.New("cipark envtest pre-launch refused")
	adapter.preLaunchErr = preErr
	if _, err := h.reconcileOutcome(); !errors.Is(err, preErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, preErr)
	}
	guarded := h.run()
	if got := guarded.Status.Phase; got != courierv1alpha1.PhasePending {
		t.Fatalf("phase after the failed pre-launch = %q, want Pending", got)
	}
	if !reflect.DeepEqual(guarded.Status.CIRepair, wantContext) {
		t.Fatalf("status.ciRepair = %#v after the failed pre-launch, want the attempt context %#v intact", guarded.Status.CIRepair, wantContext)
	}
	if guarded.Status.CISettlement != nil || len(adapter.outcomes) != 0 {
		t.Fatalf("settlement = %#v outcomes = %#v after the failed pre-launch, want no settlement and no report", guarded.Status.CISettlement, adapter.outcomes)
	}

	// Re-admission runs the repair, not the original task: the same context
	// is in place when the coordinator launches, the branch identity is
	// re-derived to the run's own published branch, and the source is asked
	// for no second reservation — the fence travels with the run.
	adapter.preLaunchErr = nil
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase after the re-admission = %q, want Running", got)
	}
	relaunched := h.run()
	if relaunched.Status.Branch != branchBefore {
		t.Fatalf("branch after re-admission = %q, want %q (the repair keeps publishing to the run's own branch)", relaunched.Status.Branch, branchBefore)
	}
	if !reflect.DeepEqual(relaunched.Status.CIRepair, wantContext) {
		t.Fatalf("status.ciRepair = %#v at the relaunch, want the attempt context %#v intact", relaunched.Status.CIRepair, wantContext)
	}
	if len(adapter.reservations) != 1 {
		t.Fatalf("repair reservations = %#v, want exactly one across the rollback", adapter.reservations)
	}
	if got := h.launchCount(h.name); got != 2 {
		t.Fatalf("launches = %d, want 2 (the initial launch and the repaired relaunch)", got)
	}

	// The relaunched session publishes nothing. The preserved attempt is what
	// lets the loop see that: the same red head is no progress, the attempt
	// ends exhausted with the fence it carried, and a red-CI resolve-issue
	// run hands off — it never reaches review on the original goal.
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the relaunched coordinator exited = %q, want Verifying", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the no-progress repair = %q, want HandedOff (red CI never settles onto review)", got)
	}
	ended := h.run()
	if ended.Status.CIPark == nil || ended.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeExhausted {
		t.Fatalf("park record after the no-progress repair = %#v, want lastOutcome exhausted", ended.Status.CIPark)
	}
	if ended.Status.CIRepair != nil {
		t.Fatalf("status.ciRepair = %#v after the attempt ended, want nil", ended.Status.CIRepair)
	}
	if ended.Status.CISettlement != nil {
		t.Fatalf("status.ciSettlement = %#v after the delivered settlement, want nil", ended.Status.CISettlement)
	}
	if len(adapter.outcomes) != 1 {
		t.Fatalf("source outcomes = %#v, want exactly one", adapter.outcomes)
	}
	outcome := adapter.outcomes[0]
	if outcome.FenceToken != granted {
		t.Fatalf("delivered outcome fence token = %q, want the granted token %q (the fence survived the rollback)", outcome.FenceToken, granted)
	}
	if outcome.IdempotencyKey != adapter.reservations[0].IdempotencyKey {
		t.Fatalf("delivered outcome idempotency key = %q, want the reservation key %q", outcome.IdempotencyKey, adapter.reservations[0].IdempotencyKey)
	}
	if outcome.Result != source.CIRepairExhausted {
		t.Fatalf("delivered outcome result = %q, want exhausted", outcome.Result)
	}
}

// TestEnvtestCIParkTerminalAbandonmentSettlesFence pins #547's abandonment
// posture: a repair-resumed run whose launch fails with a permanent
// rejection terminalizes NeedsHuman without ever reaching the park loop's
// graded endings, and the terminal write carries the unsupported settlement
// for the attempt still in flight. The following reconcile delivers it —
// a run that can never report must not leave the source deferring its
// hand-out against the granted reservation.
func TestEnvtestCIParkTerminalAbandonmentSettlesFence(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarness(t, "cipark-terminal-abandon", ciparkPolicy(2), adapter)

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	resumed := h.run()
	granted := resumed.Status.CIRepair.FenceToken
	if granted == "" {
		t.Fatal("repair fence token = empty, want the source-granted fence")
	}

	h.reconciler.Launch = func(context.Context, *courierv1alpha1.CoderRun) error {
		return secureNeedsHuman(laneConfigReason, "lane %q cannot launch this run", h.laneName)
	}
	if _, err := h.reconcileOutcome(); err != nil {
		t.Fatalf("Reconcile() error = %v, want the permanent rejection handled", err)
	}
	ended := h.run()
	if got := ended.Status.Phase; got != courierv1alpha1.PhaseNeedsHuman {
		t.Fatalf("phase after the permanent rejection = %q, want NeedsHuman", got)
	}
	if ended.Status.CIRepair != nil {
		t.Fatalf("status.ciRepair = %#v after the terminal abandonment, want nil", ended.Status.CIRepair)
	}
	if ended.Status.CISettlement == nil {
		t.Fatal("status.ciSettlement = nil after the terminal abandonment, want the unsupported settlement for the granted fence")
	}
	if ended.Status.CISettlement.FenceToken != granted {
		t.Fatalf("settlement fence token = %q, want the granted token %q", ended.Status.CISettlement.FenceToken, granted)
	}
	if ended.Status.CISettlement.Result != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("settlement result = %q, want unsupported (an abandonment publishes nothing)", ended.Status.CISettlement.Result)
	}
	if len(adapter.outcomes) != 0 {
		t.Fatalf("source outcomes = %#v at the terminal write, want the record to precede the delivery attempt", adapter.outcomes)
	}

	// The following reconcile delivers the recorded abandonment before the
	// terminal dispatch, and the cleared record is the proof of delivery.
	h.reconcile()
	delivered := h.run()
	if delivered.Status.CISettlement != nil {
		t.Fatalf("status.ciSettlement = %#v after the delivery reconcile, want nil", delivered.Status.CISettlement)
	}
	if len(adapter.outcomes) != 1 {
		t.Fatalf("source outcomes = %#v, want exactly one", adapter.outcomes)
	}
	outcome := adapter.outcomes[0]
	if outcome.FenceToken != granted {
		t.Fatalf("delivered outcome fence token = %q, want the granted token %q", outcome.FenceToken, granted)
	}
	if outcome.IdempotencyKey != adapter.reservations[0].IdempotencyKey {
		t.Fatalf("delivered outcome idempotency key = %q, want the reservation key %q", outcome.IdempotencyKey, adapter.reservations[0].IdempotencyKey)
	}
	if outcome.Result != source.CIRepairUnsupported {
		t.Fatalf("delivered outcome result = %q, want unsupported", outcome.Result)
	}
	if outcome.Head != "" {
		t.Fatalf("delivered outcome head = %q, want empty (only a repush carries a head)", outcome.Head)
	}
	if outcome.PR != "42" {
		t.Fatalf("delivered outcome PR = %q, want 42", outcome.PR)
	}
}

// TestEnvtestCIParkLostRepushSettlementReplays pins the durable settlement on
// the repush path: the repushed outcome of a repair whose report is lost is
// re-presented on the next observation before the loop decides anything
// else, so the next reservation can never race a deferral the source still
// holds.
func TestEnvtestCIParkLostRepushSettlementReplays(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &flakySettlementSource{}
	h := newCIParkHarness(t, "cipark-lost-repush", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	h.reconcile()
	h.reconcile()
	// The repair publishes a new head: the repushed outcome's report is the
	// one that gets lost.
	h.publishHead("sha-2")
	h.world.set(redObservation("sha-2", "build"))
	h.reconcile()
	parked := h.run()
	if parked.Status.CISettlement == nil {
		t.Fatal("status.ciSettlement = nil after the lost repush report, want the pending settlement")
	}
	if parked.Status.CISettlement.Result != courierv1alpha1.CIParkOutcomeRepushed {
		t.Fatalf("pending settlement result = %q, want repushed", parked.Status.CISettlement.Result)
	}
	if parked.Status.CISettlement.Head != "sha-2" {
		t.Fatalf("pending settlement head = %q, want the republished head sha-2", parked.Status.CISettlement.Head)
	}
	if parked.Status.CIPark.Head != "sha-2" {
		t.Fatalf("park head = %q, want the republished head sha-2", parked.Status.CIPark.Head)
	}
	// The next observation delivers the pending settlement before the park
	// loop decides on the run, and the loop continues on the repushed head.
	// A controller restart in between changes nothing: the pending
	// settlement is durable intent on the run.
	h.reconciler = h.newReconciler()
	h.world.set(greenObservation("sha-2", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	if len(adapter.outcomes) != 2 {
		t.Fatalf("source outcomes = %#v, want the committed repush report and its replay", adapter.outcomes)
	}
	if !reflect.DeepEqual(adapter.outcomes[0], adapter.outcomes[1]) {
		t.Fatalf("replayed outcome = %#v, want byte-identical to the committed one %#v", adapter.outcomes[1], adapter.outcomes[0])
	}
	if adapter.outcomes[0].Result != source.CIRepairRepushed || adapter.outcomes[0].Head != "sha-2" {
		t.Fatalf("delivered outcome = %#v, want the repushed outcome on sha-2", adapter.outcomes[0])
	}
	delivered := h.run()
	if delivered.Status.CISettlement != nil {
		t.Fatalf("status.ciSettlement = %#v after the replay, want nil", delivered.Status.CISettlement)
	}
}

// TestEnvtestCIParkLostNoticeReplays pins the durable park notice: a park
// notice whose response is lost leaves durable retry intent on the run, the
// next reconcile re-presents it, and the source sees byte-identical notices
// — the same deferral re-established, never double-booked — while the
// repair cycle itself proceeds unchanged.
func TestEnvtestCIParkLostNoticeReplays(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	for name, lostBeforeCommit := range map[string]bool{
		"response lost after commit": false,
		"failure before commit":      true,
	} {
		lostBeforeCommit := lostBeforeCommit
		t.Run(name, func(t *testing.T) {
			adapter := &flakySettlementSource{noticeLostBeforeCommit: lostBeforeCommit}
			h := newCIParkHarness(t, "cipark-lost-notice-"+strings.ReplaceAll(name, " ", "-"), ciparkPolicy(2), adapter)
			h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
			parked := h.run()
			if parked.Status.CIParkDeferral == nil {
				t.Fatal("status.ciparkDeferral = nil after the lost notice, want the pending notice")
			}
			if parked.Status.CIParkDeferral.Head != "sha-1" {
				t.Fatalf("pending notice head = %q, want the parked head sha-1", parked.Status.CIParkDeferral.Head)
			}
			if parked.Status.CIParkDeferral.IdempotencyKey == "" {
				t.Fatal("pending notice idempotency key = empty, want a stable key")
			}
			// The retry re-presents the same notice; the source accepts it
			// once. A controller restart in between changes nothing: the
			// pending notice is durable intent on the run, and a fresh
			// reconciler delivers it.
			h.reconciler = h.newReconciler()
			h.reconcile()
			if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
				t.Fatalf("phase after the notice replay = %q, want Verifying (the run stays parked)", got)
			}
			wantNotices := 2
			if lostBeforeCommit {
				wantNotices = 1
			}
			if len(adapter.parks) != wantNotices {
				t.Fatalf("source park notices = %#v, want %d", adapter.parks, wantNotices)
			}
			if !lostBeforeCommit && !reflect.DeepEqual(adapter.parks[0], adapter.parks[1]) {
				t.Fatalf("replayed notice = %#v, want byte-identical to the committed one %#v (the same idempotency key re-establishes one deferral)", adapter.parks[1], adapter.parks[0])
			}
			if adapter.parks[len(adapter.parks)-1].Head != "sha-1" {
				t.Fatalf("delivered notice head = %q, want sha-1", adapter.parks[len(adapter.parks)-1].Head)
			}
			delivered := h.run()
			if delivered.Status.CIParkDeferral != nil {
				t.Fatalf("status.ciparkDeferral = %#v after the replay, want nil (the clear is the proof of delivery)", delivered.Status.CIParkDeferral)
			}
			// The repair cycle is unaffected: a settled failure still wakes
			// exactly one bounded repair.
			h.settleFailure("sha-1", "build")
			if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
				t.Fatalf("phase after the settled failure = %q, want Claimed", got)
			}
			h.reconcile()
			if got := h.phase(); got != courierv1alpha1.PhaseRunning {
				t.Fatalf("phase after the resumed launch = %q, want Running", got)
			}
			if got := h.launchCount(h.name); got != 2 {
				t.Fatalf("launches = %d, want 2 (the initial launch and one repair)", got)
			}
		})
	}
}

// refusingParkSource models a source whose adapter implements the park
// capability but whose work the source cannot defer at all: the lifecycle
// reports are refused permanently with source.ErrUnsupportedCIPark, the
// sentinel a Dispatch adapter returns for implement and queue-less items.
// A refusal is permanent, so the source never records one.
type refusingParkSource struct {
	ciparkSource
	noticeRefused     bool
	settlementRefused bool
	releaseRefused    bool
}

func (s *refusingParkSource) ReportCIParked(ctx context.Context, item source.WorkItem, notice source.CIParkNotice) error {
	if s.noticeRefused {
		return source.ErrUnsupportedCIPark
	}
	return s.ciparkSource.ReportCIParked(ctx, item, notice)
}

func (s *refusingParkSource) ReportCIRepair(ctx context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	if s.settlementRefused {
		return source.ErrUnsupportedCIPark
	}
	return s.ciparkSource.ReportCIRepair(ctx, item, outcome)
}

func (s *refusingParkSource) ReleaseCIPark(_ context.Context, item source.WorkItem, release source.CIParkRelease) error {
	if s.releaseRefused {
		return source.ErrUnsupportedCIPark
	}
	return s.ciparkSource.ReleaseCIPark(context.Background(), item, release)
}

// TestEnvtestCIParkUnsupportedNoticeStopsRetrying pins the permanent refusal:
// a park notice refused with source.ErrUnsupportedCIPark drops the pending
// notice instead of retrying forever, so the run's own park loop stays
// reachable and the window can still hand the run off.
func TestEnvtestCIParkUnsupportedNoticeStopsRetrying(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &refusingParkSource{noticeRefused: true}
	h := newCIParkHarness(t, "cipark-unsupported-notice", &courierv1alpha1.CIRepairPolicy{
		MechanicalChecks:         []string{"build"},
		MaxAttempts:              1,
		ObservationWindowSeconds: 60,
	}, adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v after the permanent refusal, want nil (nothing is owed)", parked.Status.CIParkDeferral)
	}
	if len(adapter.parks) != 0 {
		t.Fatalf("source park notices = %d, want 0 (a permanent refusal is never recorded)", len(adapter.parks))
	}
	// The park loop is reachable again: the window expires and hands off
	// instead of the delivery hook holding the run parked forever.
	now = now.Add(2 * time.Minute)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the park window closed = %q, want HandedOff", got)
	}
	if len(adapter.parks) != 0 {
		t.Fatalf("source park notices = %d, want 0 after further reconciles", len(adapter.parks))
	}
}

// TestEnvtestCIParkUnsupportedSettlementStopsRetrying pins the permanent
// refusal on the settlement path: a fenced outcome refused with
// source.ErrUnsupportedCIPark drops the pending settlement instead of
// retrying forever behind a terminal run.
func TestEnvtestCIParkUnsupportedSettlementStopsRetrying(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &refusingParkSource{settlementRefused: true}
	h := newCIParkHarness(t, "cipark-unsupported-settlement", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	h.reconcile()
	h.reconcile()
	// The repair publishes nothing: the attempt ends as exhausted, and the
	// settlement carries the fence the reservation granted.
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the repair published nothing = %q, want HandedOff", got)
	}
	ended := h.run()
	if ended.Status.CISettlement != nil {
		t.Fatalf("status.ciSettlement = %#v after the permanent refusal, want nil (nothing is owed)", ended.Status.CISettlement)
	}
	if len(adapter.outcomes) != 0 {
		t.Fatalf("source outcomes = %d, want 0 (a permanent refusal is never recorded)", len(adapter.outcomes))
	}
	// The terminal run stays terminal: no reconcile re-presents anything.
	h.reconcile()
	settled := h.run()
	if got := settled.Status.Phase; got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after a further reconcile = %q, want HandedOff", got)
	}
	if settled.Status.CISettlement != nil {
		t.Fatalf("status.ciSettlement = %#v after a further reconcile, want nil", settled.Status.CISettlement)
	}
}

// mootSettlementSource models a source whose fence no longer accepts this
// run's settlement: the fenced outcome is refused with the coded refusal
// Dispatch's cirepair route answers a settled-or-vacated fence with —
// fence-settled mapped to source.ErrStaleWork (the transition is already
// committed) or fence-gone mapped to source.ErrSuperseded (the fence is
// consumed or gone). Neither can ever clear on retry, and a refusal is not
// a delivery, so nothing is recorded.
type mootSettlementSource struct {
	ciparkSource
	refusal error
}

func (s *mootSettlementSource) ReportCIRepair(_ context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	s.items = append(s.items, item)
	return s.refusal
}

// TestEnvtestCIParkMootSettlementStopsRetrying pins the moot settlement: a
// pending settlement refused fence-settled (source.ErrStaleWork) or
// fence-gone (source.ErrSuperseded) proves the fenced transition is not
// this record's to deliver any more, so the record drops as the proof of
// delivery — mirroring the release path — instead of the terminal reap gate
// re-presenting it at the settlement cadence forever. The fix-PR case pins
// the release of the Failed-run retention gate itself, where the run is
// reaped only once it owes no close.
func TestEnvtestCIParkMootSettlementStopsRetrying(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	for name, tc := range map[string]struct {
		refusal error
		mode    courierv1alpha1.Mode
		phase   courierv1alpha1.Phase
	}{
		"settled-fence":        {source.ErrStaleWork, courierv1alpha1.ModeResolveIssue, courierv1alpha1.PhaseHandedOff},
		"gone-fence":           {source.ErrSuperseded, courierv1alpha1.ModeResolveIssue, courierv1alpha1.PhaseHandedOff},
		"failed-run-retention": {source.ErrStaleWork, courierv1alpha1.ModeFixPR, courierv1alpha1.PhaseFailed},
	} {
		t.Run(name, func(t *testing.T) {
			adapter := &mootSettlementSource{refusal: tc.refusal}
			h := newCIParkHarnessMode(t, "cipark-moot-settlement-"+name, ciparkPolicy(2), adapter, tc.mode)
			if tc.mode == courierv1alpha1.ModeFixPR {
				h.reconciler.PRHeadResolver = ExistingPRHeadResolverFunc(func(_ context.Context, run *courierv1alpha1.CoderRun) (HeadRef, error) {
					return HeadRef{Repo: run.Spec.Repo, Branch: "refs/pull/14/head", SHA: "sha-1"}, nil
				})
			}
			h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
			h.settleFailure("sha-1", "build")
			h.reconcile()
			h.reconcile()
			// The repair publishes nothing: the attempt ends exhausted, and
			// the settlement carries the fence the reservation granted.
			h.world.set(redObservation("sha-1", "build"))
			h.reconcile()
			if got := h.phase(); got != tc.phase {
				t.Fatalf("phase after the repair published nothing = %q, want %s", got, tc.phase)
			}
			ended := h.run()
			if ended.Status.CISettlement != nil {
				t.Fatalf("status.ciSettlement = %#v after the moot refusal, want nil (the drop is the proof of delivery)", ended.Status.CISettlement)
			}
			if len(adapter.outcomes) != 0 {
				t.Fatalf("source outcomes = %d, want 0 (a refusal is never recorded)", len(adapter.outcomes))
			}
			// The terminal run stays terminal and owes nothing: a further
			// reconcile never re-presents the moot settlement.
			h.reconcile()
			settled := h.run()
			if got := settled.Status.Phase; got != tc.phase {
				t.Fatalf("phase after a further reconcile = %q, want %s", got, tc.phase)
			}
			if settled.Status.CISettlement != nil {
				t.Fatalf("status.ciSettlement = %#v after a further reconcile, want nil", settled.Status.CISettlement)
			}
		})
	}
}

// failingNoticeSource fails every park notice with an ordinary — retryable,
// never-permanent — error, modeling a source that answers but never accepts
// the notice.
type failingNoticeSource struct {
	ciparkSource
}

func (s *failingNoticeSource) ReportCIParked(ctx context.Context, item source.WorkItem, notice source.CIParkNotice) error {
	return errors.New("dispatch server: unavailable")
}

// TestEnvtestCIParkFailingNoticeNeverBlocksTheWindow pins the park loop's
// liveness against its own reporting: a park notice that keeps failing with
// ordinary errors is retried on the run's own cadence and never preempts the
// observation window, so a source that never accepts the notice still hands
// the run back when the window closes.
func TestEnvtestCIParkFailingNoticeNeverBlocksTheWindow(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingNoticeSource{}
	h := newCIParkHarness(t, "cipark-failing-notice", &courierv1alpha1.CIRepairPolicy{
		MechanicalChecks:         []string{"build"},
		MaxAttempts:              1,
		ObservationWindowSeconds: 60,
	}, adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
	}
	// The window closes while the notice is still undelivered: the retry
	// must not hold the run parked past it.
	now = now.Add(2 * time.Minute)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the park window closed = %q, want HandedOff", got)
	}
	if len(adapter.parks) != 0 {
		t.Fatalf("source park notices = %d, want 0 (the source never accepted one)", len(adapter.parks))
	}
}

// TestEnvtestCIParkObservationWindowExpires pins the bounded park wait: a
// configured mechanical check that never reports hands the run off instead of
// waiting for it forever, and the wait never wakes the coordinator.
func TestEnvtestCIParkObservationWindowExpires(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	name := "cipark-window"
	h := newCIParkHarness(t, name, &courierv1alpha1.CIRepairPolicy{
		MechanicalChecks:         []string{"build"},
		MaxAttempts:              1,
		ObservationWindowSeconds: 60,
	}, &admissionSource{})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	// The window closes while the check is still pending.
	now = now.Add(2 * time.Minute)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the park window closed = %q, want HandedOff", got)
	}
	run := h.run()
	if run.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the park window closed, want the park record")
	}
	if run.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("park outcome = %q, want %q", run.Status.CIPark.LastOutcome, courierv1alpha1.CIParkOutcomeUnsupported)
	}
	if got := h.launchCount(name); got != 1 {
		t.Fatalf("launch count = %d, want 1: an expired window never wakes the coordinator", got)
	}
}

// windowExpiryPolicy is the one-check policy whose window expires inside an
// envtest scenario with two reconciles, ending the park without a
// reservation — the fence-less close's own precondition (#534).
func windowExpiryPolicy() *courierv1alpha1.CIRepairPolicy {
	return &courierv1alpha1.CIRepairPolicy{
		MechanicalChecks:         []string{"build"},
		MaxAttempts:              1,
		ObservationWindowSeconds: 60,
	}
}

// advancePastWindow fast-forwards the scenario's clock past the window.
func advancePastWindow(now *time.Time, reconciler *CoderRunReconciler) {
	*now = now.Add(2 * time.Minute)
	reconciler.Now = func() time.Time { return *now }
}

// removeCIParkPolicy strips the lane's declared repair policy: the removal
// that takes a parked run out of the park loop on the ordinary path (#553).
func removeCIParkPolicy(t *testing.T, laneName string) {
	t.Helper()
	var lane courierv1alpha1.LaneProfile
	if err := envtestClient.Get(context.Background(), envtestKey(laneName), &lane); err != nil {
		t.Fatal(err)
	}
	lane.Spec.CIRepair = nil
	if err := envtestClient.Update(context.Background(), &lane); err != nil {
		t.Fatal(err)
	}
}

// TestEnvtestCIParkDeferralReleasedWithoutReservation pins the release path
// (#534): a park whose notice the source accepted ends without a reservation
// — the window expires — and the deferral is released with the fence-less
// close, keyed by the very identity the park notice carried. The pending
// release is cleared in the same pass, so the close delivered exactly once
// and further reconciles re-present nothing.
func TestEnvtestCIParkDeferralReleasedWithoutReservation(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarness(t, "cipark-deferral-release", windowExpiryPolicy(), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	if len(adapter.parks) != 1 {
		t.Fatalf("park notices = %#v, want exactly one delivered notice", adapter.parks)
	}
	advancePastWindow(&now, h.reconciler)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the park window closed = %q, want HandedOff", got)
	}
	ended := h.run()
	if ended.Status.CIPark == nil || ended.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("park record = %#v, want the unsupported outcome", ended.Status.CIPark)
	}
	if ended.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the finish, want nil (the delivery cleared it)", ended.Status.CIParkRelease)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close", adapter.releases)
	}
	release := adapter.releases[0]
	notice := adapter.parks[0]
	// The close is keyed by the park notice's own identity: the idempotency
	// key is the notice's own, the parked head is the one it announced, and
	// the pull request is the run's.
	if release.IdempotencyKey != notice.IdempotencyKey {
		t.Fatalf("release key = %q, want the parking notice's own key %q", release.IdempotencyKey, notice.IdempotencyKey)
	}
	if release.Head != "sha-1" {
		t.Fatalf("release head = %q, want the parked head sha-1", release.Head)
	}
	if release.PR != "42" {
		t.Fatalf("release PR = %q, want 42", release.PR)
	}
	// The run stays settled: further reconciles re-present nothing.
	h.reconcile()
	if len(adapter.releases) != 1 {
		t.Fatalf("releases after a further reconcile = %d, want exactly one", len(adapter.releases))
	}
	if final := h.run(); final.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after a further reconcile, want nil", final.Status.CIParkRelease)
	}
}

// TestEnvtestCIParkPendingNoticeBecomesReleaseAtFinish pins the conversion:
// a park whose notice the source never accepted still leaves durable retry
// intent for the release, and the finish clears the pending notice in the
// same write — the release, not the notice, is what over, and the close is
// advisory when the source never saw the notice. Nothing is wedged, and the
// source sees no notice at all.
func TestEnvtestCIParkPendingNoticeBecomesReleaseAtFinish(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingNoticeSource{}
	h := newCIParkHarness(t, "cipark-pending-notice-release", windowExpiryPolicy(), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
	}
	pendingKey := parked.Status.CIParkDeferral.IdempotencyKey
	advancePastWindow(&now, h.reconciler)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the park window closed = %q, want HandedOff", got)
	}
	ended := h.run()
	// The pending notice is converted, not left to drop on a later
	// reconcile: the finish write clears it together with the release.
	if ended.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v at the finish, want nil (converted into the release)", ended.Status.CIParkDeferral)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close even though the notice never landed", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != pendingKey {
		t.Fatalf("release key = %q, want the pending notice's own key %q", adapter.releases[0].IdempotencyKey, pendingKey)
	}
	// The close is advisory: the source never had a deferral of this
	// identity, and the still-delivered close clears the release record
	// either way.
	if ended.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the finish, want nil", ended.Status.CIParkRelease)
	}
	if len(adapter.parks) != 0 {
		t.Fatalf("park notices = %d, want 0 (never delivered)", len(adapter.parks))
	}
}

// TestEnvtestCIParkFinishLookupFailureKeepsTheConversionAtomic pins the
// finish's atomicity against a source lookup that cannot answer the
// capability question: the pending notice is cleared exactly when the
// fence-less release is recorded, so a failing lookup ends nothing — no
// half-conversion is persisted — and a later finish with a working lookup
// converts the notice into the release exactly once.
func TestEnvtestCIParkFinishLookupFailureKeepsTheConversionAtomic(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingNoticeSource{}
	h := newCIParkHarness(t, "cipark-finish-lookup-fails", windowExpiryPolicy(), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
	}
	pendingKey := parked.Status.CIParkDeferral.IdempotencyKey
	advancePastWindow(&now, h.reconciler)
	// A reconciler whose registry no longer names the run's source is how a
	// source lookup failure reaches the finish.
	broken := h.newReconciler()
	broken.Sources = NewSourceRegistry(map[string]source.Adapter{})
	if _, err := broken.finishCIPark(context.Background(), h.run(), PRObservation{PR: "42", Head: "sha-1"}, courierv1alpha1.CIParkOutcomeUnsupported, ""); err == nil {
		t.Fatal("finishCIPark with an unresolvable source = nil error, want the lookup error")
	}
	// Nothing was persisted: the deferral is still owed, no release record
	// exists, and the run is not ended.
	half := h.run()
	if half.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil after the failed lookup, want the pending notice (no half-conversion)")
	}
	if half.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after the failed lookup, want nil (no record without the lookup)", half.Status.CIParkRelease)
	}
	if half.Status.Phase != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the failed lookup = %q, want Verifying (the finish did not end the run)", half.Status.Phase)
	}
	// With the lookup working again, the finish converts the notice into
	// the release exactly once, and the close carries the notice's key.
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the retried finish = %q, want HandedOff", got)
	}
	ended := h.run()
	if ended.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v after the retried finish, want nil (converted into the release)", ended.Status.CIParkDeferral)
	}
	if ended.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the finish, want nil (the delivery cleared it)", ended.Status.CIParkRelease)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close keyed by the pending notice", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != pendingKey {
		t.Fatalf("release key = %q, want the pending notice's own key %q", adapter.releases[0].IdempotencyKey, pendingKey)
	}
}

// TestEnvtestCIParkLostReleaseReplays pins the durable release: a close
// whose response is lost after the source committed it leaves the pending
// release on the run — the key travels with the record — a controller
// restart changes nothing, and the re-presented close is byte-identical, so
// the source's own idempotency collapses the retry into the one committed
// close.
func TestEnvtestCIParkLostReleaseReplays(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &flakyReleaseSource{}
	h := newCIParkHarness(t, "cipark-lost-release", windowExpiryPolicy(), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	advancePastWindow(&now, h.reconciler)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the park window closed = %q, want HandedOff", got)
	}
	ended := h.run()
	if ended.Status.CIParkRelease == nil {
		t.Fatal("status.ciparkRelease = nil after the lost close, want the pending release")
	}
	if ended.Status.CIParkRelease.Head != "sha-1" {
		t.Fatalf("pending release head = %q, want the parked head sha-1", ended.Status.CIParkRelease.Head)
	}
	pendingKey := ended.Status.CIParkRelease.IdempotencyKey
	if pendingKey == "" {
		t.Fatal("pending release idempotency key = empty, want the park notice's key")
	}
	// A controller restart changes nothing: the pending release is durable
	// intent on the run, and a fresh reconciler re-presents it byte-identical.
	h.reconciler = h.newReconciler()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the release replay = %q, want HandedOff", got)
	}
	delivered := h.run()
	if delivered.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after the replay, want nil (the clear is the proof of delivery)", delivered.Status.CIParkRelease)
	}
	if len(adapter.releases) != 2 {
		t.Fatalf("source releases = %#v, want the committed close and its replay", adapter.releases)
	}
	if adapter.releases[0] != adapter.releases[1] {
		t.Fatalf("replayed release = %#v, want byte-identical to the committed one %#v (the same key collapses into one close)", adapter.releases[1], adapter.releases[0])
	}
	// The settled run stays settled: further reconciles re-deliver nothing.
	h.reconcile()
	if len(adapter.releases) != 2 {
		t.Fatalf("source releases after a further reconcile = %d, want 2", len(adapter.releases))
	}
}

// TestEnvtestCIParkGreenSettlementReleasesDeferral pins the green ending of
// the release path (#534): a park whose notice the source accepted ends
// green — a park that ended with no reservation, so no fenced outcome will
// ever release the deferral — and the deferral is released with the
// fence-less close keyed by the very identity the park notice carried,
// before the run reaches review. The delivery clears the pending release in
// the same pass, and further reconciles re-present nothing.
func TestEnvtestCIParkGreenSettlementReleasesDeferral(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarness(t, "cipark-green-release", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	if len(adapter.parks) != 1 {
		t.Fatalf("park notices = %#v, want exactly one delivered notice", adapter.parks)
	}
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the first green observation = %q, want Verifying", got)
	}
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	ended := h.run()
	if ended.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v at the green settle, want nil", ended.Status.CIParkDeferral)
	}
	if ended.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the green settle, want nil (the delivery cleared it)", ended.Status.CIParkRelease)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close", adapter.releases)
	}
	release := adapter.releases[0]
	notice := adapter.parks[0]
	if release.IdempotencyKey != notice.IdempotencyKey {
		t.Fatalf("release key = %q, want the parking notice's own key %q", release.IdempotencyKey, notice.IdempotencyKey)
	}
	if release.Head != "sha-1" {
		t.Fatalf("release head = %q, want the parked head sha-1", release.Head)
	}
	if release.PR != "42" {
		t.Fatalf("release PR = %q, want 42", release.PR)
	}
	h.reconcile()
	if len(adapter.releases) != 1 {
		t.Fatalf("releases after a further reconcile = %d, want exactly one", len(adapter.releases))
	}
}

// TestEnvtestCIParkGreenSettlementConvertsPendingNotice pins the conversion
// on the green ending: a park whose notice the source never accepted still
// leaves durable close intent when the checks settle green, the pending
// notice is cleared in the same write as the release, and the close carries
// the pending notice's own key — advisory, since the source never saw the
// notice.
func TestEnvtestCIParkGreenSettlementConvertsPendingNotice(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingNoticeSource{}
	h := newCIParkHarness(t, "cipark-green-pending-release", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
	}
	pendingKey := parked.Status.CIParkDeferral.IdempotencyKey
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	ended := h.run()
	if ended.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v at the green settle, want nil (converted into the release)", ended.Status.CIParkDeferral)
	}
	if ended.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the green settle, want nil (the delivery cleared it)", ended.Status.CIParkRelease)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close even though the notice never landed", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != pendingKey {
		t.Fatalf("release key = %q, want the pending notice's own key %q", adapter.releases[0].IdempotencyKey, pendingKey)
	}
	if len(adapter.parks) != 0 {
		t.Fatalf("park notices = %d, want 0 (never delivered)", len(adapter.parks))
	}
}

// TestEnvtestCIParkLostGreenReleaseReplays pins the green ending's durable
// close: a release whose response is lost at the green settle never holds
// the run's ending — the review transition completes with the close still
// pending — and the review-phase run re-presents it byte-identical across a
// controller restart, the same key collapsing into the one committed close.
func TestEnvtestCIParkLostGreenReleaseReplays(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &flakyReleaseSource{}
	h := newCIParkHarness(t, "cipark-lost-green-release", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview (the ending never waits on the close)", got)
	}
	ended := h.run()
	if ended.Status.CIParkRelease == nil {
		t.Fatal("status.ciparkRelease = nil after the lost close, want the pending release")
	}
	if ended.Status.CIParkRelease.Head != "sha-1" {
		t.Fatalf("pending release head = %q, want the parked head sha-1", ended.Status.CIParkRelease.Head)
	}
	if ended.Status.CIParkRelease.IdempotencyKey != adapter.parks[0].IdempotencyKey {
		t.Fatalf("pending release key = %q, want the park notice's own key %q", ended.Status.CIParkRelease.IdempotencyKey, adapter.parks[0].IdempotencyKey)
	}
	// A controller restart changes nothing: the pending close is durable
	// intent on the review-phase run, and a fresh reconciler re-presents it
	// byte-identical.
	h.reconciler = h.newReconciler()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the release replay = %q, want AwaitingReview", got)
	}
	delivered := h.run()
	if delivered.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after the replay, want nil (the clear is the proof of delivery)", delivered.Status.CIParkRelease)
	}
	if len(adapter.releases) != 2 {
		t.Fatalf("source releases = %#v, want the committed close and its replay", adapter.releases)
	}
	if adapter.releases[0] != adapter.releases[1] {
		t.Fatalf("replayed release = %#v, want byte-identical to the committed one %#v (the same key collapses into one close)", adapter.releases[1], adapter.releases[0])
	}
	// The settled run stays settled: further reconciles re-deliver nothing.
	h.reconcile()
	if len(adapter.releases) != 2 {
		t.Fatalf("source releases after a further reconcile = %d, want 2", len(adapter.releases))
	}
}

// TestEnvtestCIParkGreenLookupFailureKeepsTheConversionAtomic pins the green
// settle's atomicity against a source lookup that cannot answer the
// capability question: the green ending persists no half-conversion — the
// deferral stays and no release record appears — and a later green settle
// with a working lookup converts exactly once.
func TestEnvtestCIParkGreenLookupFailureKeepsTheConversionAtomic(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingNoticeSource{}
	h := newCIParkHarness(t, "cipark-green-lookup-fails", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
	}
	pendingKey := parked.Status.CIParkDeferral.IdempotencyKey
	// A reconciler whose registry no longer names the run's source is how a
	// source lookup failure reaches the green settle.
	broken := h.newReconciler()
	broken.Sources = NewSourceRegistry(map[string]source.Adapter{})
	if _, err := broken.settleCIParkGreen(context.Background(), h.run(), greenObservation("sha-1", "build"), true); err == nil {
		t.Fatal("settleCIParkGreen with an unresolvable source = nil error, want the lookup error")
	}
	// Nothing was persisted: the deferral is still owed, no release record
	// exists, and the run is not ended.
	half := h.run()
	if half.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil after the failed lookup, want the pending notice (no half-conversion)")
	}
	if half.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after the failed lookup, want nil (no record without the lookup)", half.Status.CIParkRelease)
	}
	if half.Status.Phase != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the failed lookup = %q, want Verifying (the green settle did not end the run)", half.Status.Phase)
	}
	// With the lookup working again, the green settle converts the notice
	// into the release exactly once, and the close carries the notice's key.
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the retried green settle = %q, want AwaitingReview", got)
	}
	ended := h.run()
	if ended.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v after the retried green settle, want nil (converted into the release)", ended.Status.CIParkDeferral)
	}
	if ended.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the green settle, want nil (the delivery cleared it)", ended.Status.CIParkRelease)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close keyed by the pending notice", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != pendingKey {
		t.Fatalf("release key = %q, want the pending notice's own key %q", adapter.releases[0].IdempotencyKey, pendingKey)
	}
}

// TestEnvtestCIParkNoChangeGreenStillReleasesDeferral pins the green
// ending's other terminal branch: a fix-PR run whose checks settle green on
// a declared no-change blocks on authoritative feedback resolution instead
// of claiming a push — and that ending, too, is a park that ended without a
// reservation, so the deferral is released before the run blocks.
func TestEnvtestCIParkNoChangeGreenStillReleasesDeferral(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarnessMode(t, "cipark-nochange-green", ciparkPolicy(2), adapter, courierv1alpha1.ModeFixPR)
	h.reconciler.PRHeadResolver = ExistingPRHeadResolverFunc(func(_ context.Context, run *courierv1alpha1.CoderRun) (HeadRef, error) {
		return HeadRef{Repo: run.Spec.Repo, Branch: "refs/pull/14/head", SHA: "sha-1"}, nil
	})
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	declared := h.run()
	declared.Status.DeclaredNoChange = true
	if err := envtestClient.Status().Update(context.Background(), declared); err != nil {
		t.Fatal(err)
	}
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseBlockedExternal {
		t.Fatalf("phase after the settled green no-change observation = %q, want BlockedExternal", got)
	}
	ended := h.run()
	if ended.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v at the blocked ending, want nil", ended.Status.CIParkDeferral)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != adapter.parks[0].IdempotencyKey {
		t.Fatalf("release key = %q, want the parking notice's own key %q", adapter.releases[0].IdempotencyKey, adapter.parks[0].IdempotencyKey)
	}
	if ended.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the blocked ending, want nil (the delivery cleared it)", ended.Status.CIParkRelease)
	}
}

// TestEnvtestCIParkFencedSettledEndNeverReportsRelease pins the closed
// boundary of the release: an ending that carries the reservation's fence
// keeps the fenced outcome as its only route, and no fence-less close is
// converted on its behalf.
func TestEnvtestCIParkFencedSettledEndNeverReportsRelease(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarness(t, "cipark-fenced-end", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase after the settled failure = %q, want Claimed", got)
	}
	h.reconcile()
	h.reconcile()
	// The repair publishes nothing: the attempt ends as exhausted with the
	// fence the reservation granted — the fenced outcome releases the
	// deferral, not a fence-less close.
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the repair published nothing = %q, want HandedOff", got)
	}
	if len(adapter.releases) != 0 {
		t.Fatalf("releases = %d, want 0 (a fenced ending carries no release)", len(adapter.releases))
	}
	if len(adapter.outcomes) != 1 {
		t.Fatalf("outcomes = %d, want exactly one fenced outcome", len(adapter.outcomes))
	}
}

// TestEnvtestCIParkReleaseRefusalsResolveMoot pins the advisory refusals on
// the release path: a close that the source refuses because the deferral is
// already gone, is owned by a newer generation or a granted reservation, or
// cannot be represented at all drops the pending release instead of
// retrying forever behind a terminal run — the run is over, and nothing it
// can do changes the deferral's fate.
func TestEnvtestCIParkReleaseRefusalsResolveMoot(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	t.Run("deferral-already-gone", func(t *testing.T) {
		adapter := &staleReleaseSource{}
		h := newCIParkHarness(t, "cipark-release-stale", windowExpiryPolicy(), adapter)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		h.reconciler.Now = func() time.Time { return now }
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		advancePastWindow(&now, h.reconciler)
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after the refused close = %q, want HandedOff (the refusal is advisory)", got)
		}
		if got := h.run().Status.CIParkRelease; got != nil {
			t.Fatalf("status.ciparkRelease = %#v after the refusal, want nil (dropped)", got)
		}
		if got := h.run().Status.CIParkDeferral; got != nil {
			t.Fatalf("status.ciparkDeferral = %#v after the finish, want nil", got)
		}
		if len(adapter.parks) != 1 {
			t.Fatalf("park notices = %d, want 1 (the notice was delivered)", len(adapter.parks))
		}
	})
	t.Run("deferral-not-ours", func(t *testing.T) {
		adapter := &supersededReleaseSource{}
		h := newCIParkHarness(t, "cipark-release-superseded", windowExpiryPolicy(), adapter)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		h.reconciler.Now = func() time.Time { return now }
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		advancePastWindow(&now, h.reconciler)
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after the close = %q, want HandedOff", got)
		}
		if got := h.run().Status.CIParkRelease; got != nil {
			t.Fatalf("status.ciparkRelease = %#v after the refusal, want nil (dropped)", got)
		}
	})
	t.Run("release-unsupported", func(t *testing.T) {
		adapter := &refusingParkSource{releaseRefused: true}
		h := newCIParkHarness(t, "cipark-release-unsupported", windowExpiryPolicy(), adapter)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		h.reconciler.Now = func() time.Time { return now }
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		advancePastWindow(&now, h.reconciler)
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
			t.Fatalf("phase after the refused close = %q, want HandedOff (the refusal is permanent)", got)
		}
		if got := h.run().Status.CIParkRelease; got != nil {
			t.Fatalf("status.ciparkRelease = %#v after the refusal, want nil (dropped)", got)
		}
		if len(adapter.releases) != 0 {
			t.Fatalf("releases = %d, want 0 (a permanent refusal never records)", len(adapter.releases))
		}
	})
}

// TestEnvtestCIParkAbandonedParkReleasesAcceptedDeferral pins the abandon
// (#553): a run that leaves the park loop without going through a park
// ending — because the lane's repair policy was removed, or because the
// pull request is no longer parkable — releases the accepted deferral with
// the fence-less close keyed by the park notice's own identity, instead of
// leaving the source deferred against an opportunity that is over while the
// ordinary path transitions the run. The abandon itself fires once: the
// cleared park record fences every later entry, on any reconcile, from any
// controller incarnation.
func TestEnvtestCIParkAbandonedParkReleasesAcceptedDeferral(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}

	t.Run("policy-removed", func(t *testing.T) {
		adapter := &ciparkSource{}
		h := newCIParkHarness(t, "cipark-abandon-policy", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		if len(adapter.parks) != 1 {
			t.Fatalf("park notices = %#v, want exactly one delivered notice", adapter.parks)
		}
		removeCIParkPolicy(t, h.laneName)
		h.reconcile()
		// The ordinary path takes over with its pending observation, but
		// only after the abandon converted the accepted deferral into the
		// fence-less close.
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the abandon = %q, want Verifying (the ordinary path stays on it)", got)
		}
		if len(adapter.releases) != 1 {
			t.Fatalf("releases = %#v, want exactly one close for the abandoned park", adapter.releases)
		}
		delivered := adapter.releases[0]
		if delivered.Head != "sha-1" {
			t.Fatalf("close head = %q, want the parked head sha-1", delivered.Head)
		}
		if delivered.IdempotencyKey != adapter.parks[0].IdempotencyKey {
			t.Fatalf("close key = %q, want the park notice's own key %q", delivered.IdempotencyKey, adapter.parks[0].IdempotencyKey)
		}
		abandoned := h.run()
		if abandoned.Status.CIPark != nil {
			t.Fatalf("status.cipark = %#v after the abandon, want nil (the record's absence fences a re-conversion)", abandoned.Status.CIPark)
		}
		if abandoned.Status.CIParkDeferral != nil {
			t.Fatalf("status.ciparkDeferral = %#v after the abandon, want nil (converted into the close)", abandoned.Status.CIParkDeferral)
		}
		if abandoned.Status.CIParkRelease != nil {
			t.Fatalf("status.ciparkRelease = %#v after the abandon, want nil (the delivery cleared it)", abandoned.Status.CIParkRelease)
		}

		// The ordinary path settles the abandoned run on its own terms: two
		// consecutive green observations, exactly as if no policy was ever
		// declared.
		h.world.set(greenObservation("sha-1", "build"))
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the first green observation = %q, want Verifying", got)
		}
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
			t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
		}
		if got := h.launchCount(h.name); got != 1 {
			t.Fatalf("launches = %d, want 1 (the abandoned park never woke a coordinator)", got)
		}

		// The delivered close never re-fires: not on later reconciles, and
		// not across a controller restart.
		h.reconcile()
		h.reconciler = h.newReconciler()
		h.reconcile()
		if len(adapter.releases) != 1 {
			t.Fatalf("releases after the terminal settle = %d, want exactly one", len(adapter.releases))
		}
		if final := h.run(); final.Status.CIPark != nil || final.Status.CIParkRelease != nil || final.Status.CIParkDeferral != nil {
			t.Fatalf("run status after the restart = park %#v deferral %#v release %#v, want all nil", final.Status.CIPark, final.Status.CIParkDeferral, final.Status.CIParkRelease)
		}
	})

	t.Run("pull-request-unparkable", func(t *testing.T) {
		adapter := &ciparkSource{}
		h := newCIParkHarness(t, "cipark-abandon-draft", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		if len(adapter.parks) != 1 {
			t.Fatalf("park notices = %#v, want exactly one delivered notice", adapter.parks)
		}
		// The pull request the park established itself on is now a draft: the
		// ordinary path owns the observation and transitions NeedsHuman.
		h.world.set(PRObservation{PR: "42", Head: "sha-1", Draft: true})
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseNeedsHuman {
			t.Fatalf("phase after the draft observation = %q, want NeedsHuman", got)
		}
		if len(adapter.releases) != 1 {
			t.Fatalf("releases = %#v, want exactly one close for the abandoned park", adapter.releases)
		}
		if adapter.releases[0].IdempotencyKey != adapter.parks[0].IdempotencyKey {
			t.Fatalf("close key = %q, want the park notice's own key %q", adapter.releases[0].IdempotencyKey, adapter.parks[0].IdempotencyKey)
		}
		if head := adapter.releases[0].Head; head != "sha-1" {
			t.Fatalf("close head = %q, want the parked head sha-1", head)
		}
		abandoned := h.run()
		if abandoned.Status.CIPark != nil || abandoned.Status.CIParkDeferral != nil || abandoned.Status.CIParkRelease != nil {
			t.Fatalf("run status after the abandon = park %#v deferral %#v release %#v, want all nil", abandoned.Status.CIPark, abandoned.Status.CIParkDeferral, abandoned.Status.CIParkRelease)
		}
		if got := h.launchCount(h.name); got != 1 {
			t.Fatalf("launches = %d, want 1 (the abandoned park never woke a coordinator)", got)
		}
		// The terminal run re-presents nothing, on any later reconcile or
		// controller restart.
		h.reconcile()
		h.reconciler = h.newReconciler()
		h.reconcile()
		if len(adapter.releases) != 1 {
			t.Fatalf("releases after further reconciles = %d, want exactly one", len(adapter.releases))
		}
		if final := h.run(); final.Status.CIPark != nil || final.Status.CIParkRelease != nil || final.Status.CIParkDeferral != nil {
			t.Fatalf("run status after the restart = park %#v deferral %#v release %#v, want all nil", final.Status.CIPark, final.Status.CIParkDeferral, final.Status.CIParkRelease)
		}
	})

	// The lost-close variant: the source commits the close and loses the
	// response, so the abandon leaves durable retry intent — re-presented
	// byte-identical on the run's next reconcile, the same key collapsing
	// into the one committed close — and the ending never waits on it.
	t.Run("policy-removed-replays-lost-close", func(t *testing.T) {
		adapter := &flakyReleaseSource{}
		h := newCIParkHarness(t, "cipark-abandon-lost-close", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		if len(adapter.parks) != 1 {
			t.Fatalf("park notices = %#v, want exactly one delivered notice", adapter.parks)
		}
		removeCIParkPolicy(t, h.laneName)
		h.reconcile()
		pendingRelease := h.run().Status.CIParkRelease
		if pendingRelease == nil {
			t.Fatal("status.ciparkRelease = nil after the lost close, want the pending release")
		}
		if pendingRelease.IdempotencyKey != adapter.parks[0].IdempotencyKey {
			t.Fatalf("pending release key = %q, want the park notice's own key %q", pendingRelease.IdempotencyKey, adapter.parks[0].IdempotencyKey)
		}
		// The next reconcile re-presents the close byte-identical, before the
		// ordinary path has settled anything.
		h.reconcile()
		if len(adapter.releases) != 2 {
			t.Fatalf("releases = %#v, want the committed close and its replay", adapter.releases)
		}
		if adapter.releases[0] != adapter.releases[1] {
			t.Fatalf("replayed close = %#v, want byte-identical to the committed one %#v (the same key collapses into one close)", adapter.releases[1], adapter.releases[0])
		}
		if final := h.run(); final.Status.CIParkRelease != nil {
			t.Fatalf("status.ciparkRelease = %#v after the replay, want nil (the clear is the proof of delivery)", final.Status.CIParkRelease)
		}
	})
}

// TestEnvtestCIParkAbandonedParkWithoutDeferralRecordsNothing pins the gated
// boundary of the abandon: a source without the CIParkLifecycle capability
// never had a notice recorded for the park — there is no accepted deferral
// and nothing to close — so the abandoned park records and delivers nothing,
// and the ordinary path terminalizes the run with its lifecycle untouched.
func TestEnvtestCIParkAbandonedParkWithoutDeferralRecordsNothing(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	// admissionSource is the plain stub that predates the park capability:
	// implementing no CIParkLifecycle method is the gate itself.
	adapter := &admissionSource{}
	h := newCIParkHarness(t, "cipark-abandon-plain-source", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIPark == nil {
		t.Fatal("status.cipark = nil while parked, want the park record")
	}
	if parked.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v on a source without the capability, want nil (no notice was ever recorded)", parked.Status.CIParkDeferral)
	}
	// The ordinary surface saw the claim and the launch, and nothing else.
	if len(adapter.claimed) != 1 {
		t.Fatalf("source claims = %#v, want exactly one (the plain source never hears about a park)", adapter.claimed)
	}
	removeCIParkPolicy(t, h.laneName)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the abandon = %q, want Verifying (the ordinary path stays on it)", got)
	}
	abandoned := h.run()
	if abandoned.Status.CIPark != nil {
		t.Fatalf("status.cipark = %#v after the abandon, want nil", abandoned.Status.CIPark)
	}
	if abandoned.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after the abandon, want nil (nothing was owed)", abandoned.Status.CIParkRelease)
	}
	// The abandon called the source for nothing: only the park capability
	// ever would have been asked, and the plain source has none.
	if len(adapter.claimed) != 1 || len(adapter.transitions) != 1 {
		t.Fatalf("source surface after the abandon = claims %#v transitions %#v, want launch-only (claims one, in-progress once)", adapter.claimed, adapter.transitions)
	}
	if len(adapter.reports) != 0 {
		t.Fatalf("source reports after the abandon = %#v, want none before the terminal settle", adapter.reports)
	}

	// The ordinary path terminalizes with its own lifecycle, exactly as a
	// never-parked run would.
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	final := h.run()
	if final.Status.CIPark != nil || final.Status.CIParkRelease != nil || final.Status.CIParkDeferral != nil {
		t.Fatalf("run status at the terminal settle = park %#v deferral %#v release %#v, want all nil", final.Status.CIPark, final.Status.CIParkDeferral, final.Status.CIParkRelease)
	}
	if len(adapter.reports) != 1 {
		t.Fatalf("source reports at the terminal settle = %#v, want exactly one (the ordinary ending's own report)", adapter.reports)
	}
}

// TestEnvtestCIParkAbandonedParkConvertsPendingNotice pins the conversion
// the abandon inherits from the finish (#545): a park whose notice the
// source never accepted still leaves durable close intent when the loop
// abandons the run, and the abandon clears the pending notice in the same
// write as the release — the close, not the notice, is what is owed once the
// opportunity is over; advisory when the notice never landed (#534).
func TestEnvtestCIParkAbandonedParkConvertsPendingNotice(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingNoticeSource{}
	h := newCIParkHarness(t, "cipark-abandon-pending-notice", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
	}
	pendingKey := parked.Status.CIParkDeferral.IdempotencyKey
	removeCIParkPolicy(t, h.laneName)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the abandon = %q, want Verifying (the ordinary path stays on it)", got)
	}
	abandoned := h.run()
	if abandoned.Status.CIPark != nil {
		t.Fatalf("status.cipark = %#v after the abandon, want nil (the record's absence fences a re-conversion)", abandoned.Status.CIPark)
	}
	if abandoned.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v after the abandon, want nil (converted into the close)", abandoned.Status.CIParkDeferral)
	}
	if abandoned.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after the abandon, want nil (the delivery cleared it)", abandoned.Status.CIParkRelease)
	}
	// The close is what converts the never-accepted pending notice into, and
	// it carries the notice's own key.
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close even though the notice never landed", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != pendingKey {
		t.Fatalf("close key = %q, want the pending notice's own key %q", adapter.releases[0].IdempotencyKey, pendingKey)
	}
	if len(adapter.parks) != 0 {
		t.Fatalf("park notices = %d, want 0 (the source never accepted one)", len(adapter.parks))
	}

	// The ordinary path terminalizes, and the delivered close never re-fires.
	h.world.set(greenObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
	}
	h.reconcile()
	if len(adapter.releases) != 1 {
		t.Fatalf("releases after the terminal settle = %d, want exactly one", len(adapter.releases))
	}
}

// TestEnvtestCIParkAbandonedParkWithLiveFenceSettlesTheReservation pins the
// abandon's armed branch (#553): a repair attempt whose coordinator just
// exited reaches Verifying with its fence still live — the exit handler
// writes the phase alone and the loop clears the context only when its next
// observation consumes the attempt — and an abandon reaching that
// observation settles the reservation exactly as the loop's own endings
// would. The mapped outcome rides the fence as durable intent while the
// attempt context, the pending notice, and the park record die with it in
// the same write; no fence-less close is recorded on its behalf, because a
// granted reservation is never released fence-less. The ordinary path takes
// over on the same observation, and the delivered settlement never re-fires.
func TestEnvtestCIParkAbandonedParkWithLiveFenceSettlesTheReservation(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}

	// abandonedWithLiveFence drives one flow both bail-outs share: the run
	// parks, a settled failure wakes the coordinator for one fence-backed
	// repair, the coordinator exits back to Verifying with the fence still
	// live, and the named trigger walks the run out of the loop under that
	// fence. It returns the harness and the fence the attempt carried, so
	// the subtests can tie the surfaces they assert on to that fence.
	abandonedWithLiveFence := func(t *testing.T, name string, adapter source.Adapter, trigger func(t *testing.T, h *ciparkHarness)) (*ciparkHarness, string) {
		t.Helper()
		h := newCIParkHarness(t, name, ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		h.settleFailure("sha-1", "build")
		// The settled failure wakes the run for one repair; the reservation
		// is fenced.
		if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
			t.Fatalf("phase after the settled failure = %q, want Claimed", got)
		}
		resumed := h.run()
		if resumed.Status.CIRepair == nil {
			t.Fatal("status.ciRepair = nil after the resume, want the repair context")
		}
		if resumed.Status.CIRepair.FenceToken == "" {
			t.Fatal("repair fence token = empty after the resume, want the grant the reserve returned")
		}
		attemptFence := resumed.Status.CIRepair.FenceToken
		// The resumed coordinator runs and exits; the phase write carries the
		// attempt context across untouched — Verifying is exactly where a
		// live fence is observable.
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseRunning {
			t.Fatalf("phase after the resumed launch = %q, want Running", got)
		}
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the resumed coordinator exited = %q, want Verifying", got)
		}
		if exited := h.run(); exited.Status.CIRepair == nil || exited.Status.CIRepair.FenceToken != attemptFence {
			t.Fatalf("attempt context after the exit = %+v, want the attempt and its fence still live", exited.Status.CIRepair)
		}
		// The abort trigger works from a calm pending world, so the ordinary
		// path it hands to does not settle the still-red observation in the
		// same reconcile.
		h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
		trigger(t, h)
		h.reconcile()
		return h, attemptFence
	}

	t.Run("policy-removed", func(t *testing.T) {
		adapter := &ciparkSource{}
		h, attemptFence := abandonedWithLiveFence(t, "cipark-abandon-fenced-policy", adapter, func(t *testing.T, h *ciparkHarness) {
			removeCIParkPolicy(t, h.laneName)
		})
		if attemptFence != adapter.reservations[0].FenceToken {
			t.Fatalf("repair fence token = %q, want the reserved fence %q", attemptFence, adapter.reservations[0].FenceToken)
		}
		// The armed abandon settles the reservation: the mapped no-progress
		// outcome (the attempt published nothing on its head) rides the
		// fence as durable intent, and the attempt context dies with it.
		// The record is nil by the time this read lands — the delivery in
		// the same reconcile answered, and the clear is the proof of
		// delivery; the lost-response subtest below pins the record's
		// pending state.
		abandoned := h.run()
		if abandoned.Status.CISettlement != nil {
			t.Fatalf("status.ciSettlement = %#v after the abandon, want nil (the delivery cleared it)", abandoned.Status.CISettlement)
		}
		if abandoned.Status.CIRepair != nil {
			t.Fatalf("status.ciRepair = %#v after the abandon, want nil (the attempt is settled)", abandoned.Status.CIRepair)
		}
		if abandoned.Status.CIPark != nil {
			t.Fatalf("status.cipark = %#v after the abandon, want nil (uniform fire-once fence)", abandoned.Status.CIPark)
		}
		if abandoned.Status.CIParkDeferral != nil {
			t.Fatalf("status.ciparkDeferral = %#v after the abandon, want nil", abandoned.Status.CIParkDeferral)
		}
		if abandoned.Status.CIParkRelease != nil {
			t.Fatalf("status.ciparkRelease = %#v after the abandon, want nil (a fenced outcome is the release; no close is recorded)", abandoned.Status.CIParkRelease)
		}
		// The settlement was delivered, carrying the reservation's fence and
		// the mapped result; the close route is never touched.
		if len(adapter.outcomes) != 1 {
			t.Fatalf("source outcomes = %#v, want exactly one settled fence", adapter.outcomes)
		}
		outcome := adapter.outcomes[0]
		if outcome.FenceToken != adapter.reservations[0].FenceToken {
			t.Fatalf("delivered fence = %q, want the reserved fence %q", outcome.FenceToken, adapter.reservations[0].FenceToken)
		}
		if outcome.Result != source.CIRepairExhausted {
			t.Fatalf("delivered result = %q, want %q", outcome.Result, source.CIRepairExhausted)
		}
		if outcome.Head != "" {
			t.Fatalf("delivered head = %q, want none (only a repush carries one)", outcome.Head)
		}
		if len(adapter.releases) != 0 {
			t.Fatalf("releases = %d, want 0 (the fenced outcome releases the deferral)", len(adapter.releases))
		}

		// The ordinary path proceeds on its own terms: two consecutive green
		// observations, with the abandoned park never re-engaged and the
		// settled reservation never re-reported.
		h.world.set(greenObservation("sha-1", "build"))
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the first green observation = %q, want Verifying", got)
		}
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
			t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
		}
		if got := h.launchCount(h.name); got != 2 {
			t.Fatalf("launches = %d, want 2 (the wake plus the attempted repair stand; the abandon wakes nobody)", got)
		}
		h.reconcile()
		h.reconciler = h.newReconciler()
		h.reconcile()
		if len(adapter.outcomes) != 1 || len(adapter.releases) != 0 {
			t.Fatalf("source surface after the terminal settle = %d outcomes/%d releases, want 1/0", len(adapter.outcomes), len(adapter.releases))
		}
		if final := h.run(); final.Status.CIPark != nil || final.Status.CIRepair != nil || final.Status.CISettlement != nil || final.Status.CIParkDeferral != nil || final.Status.CIParkRelease != nil {
			t.Fatalf("run status after the restart = park %#v attempt %#v settlement %#v deferral %#v release %#v, want all nil", final.Status.CIPark, final.Status.CIRepair, final.Status.CISettlement, final.Status.CIParkDeferral, final.Status.CIParkRelease)
		}
	})

	t.Run("pull-request-unparkable", func(t *testing.T) {
		adapter := &ciparkSource{}
		h, attemptFence := abandonedWithLiveFence(t, "cipark-abandon-fenced-draft", adapter, func(t *testing.T, h *ciparkHarness) {
			// The policy stands; the pull request went draft mid-attempt —
			// the second bail-out owns the run now.
			h.world.set(PRObservation{PR: "42", Head: "sha-1", Draft: true})
		})
		if attemptFence != adapter.reservations[0].FenceToken {
			t.Fatalf("repair fence token = %q, want the reserved fence %q", attemptFence, adapter.reservations[0].FenceToken)
		}
		// The armed abandon settles under the draft observation, and the
		// ordinary path turns the run NeedsHuman exactly as an unparkable
		// request always does.
		if got := h.phase(); got != courierv1alpha1.PhaseNeedsHuman {
			t.Fatalf("phase after the abandon = %q, want NeedsHuman", got)
		}
		abandoned := h.run()
		// The delivered settlement left its record cleared (the clear is the
		// proof of delivery), so the delivered outcome is asserted on the
		// source's own recording; only the lost-response subtest sees the
		// record pending.
		if abandoned.Status.CISettlement != nil {
			t.Fatalf("status.ciSettlement = %#v after the abandon, want nil (the delivery cleared it)", abandoned.Status.CISettlement)
		}
		if abandoned.Status.CIRepair != nil || abandoned.Status.CIPark != nil || abandoned.Status.CIParkDeferral != nil || abandoned.Status.CIParkRelease != nil {
			t.Fatalf("run status after the abandon = attempt %#v park %#v deferral %#v release %#v, want all nil", abandoned.Status.CIRepair, abandoned.Status.CIPark, abandoned.Status.CIParkDeferral, abandoned.Status.CIParkRelease)
		}
		if len(adapter.outcomes) != 1 || len(adapter.releases) != 0 {
			t.Fatalf("source surface = %d outcomes/%d releases, want 1/0 (the fenced outcome is the release)", len(adapter.outcomes), len(adapter.releases))
		}
		if outcome := adapter.outcomes[0]; outcome.Result != source.CIRepairExhausted || outcome.FenceToken != attemptFence || outcome.Head != "" {
			t.Fatalf("delivered outcome = %#v, want the reserved fence settled exhausted with no head", outcome)
		}
		// The terminal run re-reports nothing, across reconciles and a
		// restart.
		h.reconcile()
		h.reconciler = h.newReconciler()
		h.reconcile()
		if len(adapter.outcomes) != 1 || len(adapter.releases) != 0 {
			t.Fatalf("source surface after further reconciles = %d outcomes/%d releases, want 1/0", len(adapter.outcomes), len(adapter.releases))
		}
	})

	// The armed abandon's durable intent: the settlement's commit lost its
	// response, so the record rides the fence it presents and a later
	// reconcile re-presents it byte-identical, park record gone or not.
	t.Run("lost-settlement-replays", func(t *testing.T) {
		adapter := &flakySettlementSource{}
		h, attemptFence := abandonedWithLiveFence(t, "cipark-abandon-fenced-lost", adapter, func(t *testing.T, h *ciparkHarness) {
			removeCIParkPolicy(t, h.laneName)
		})
		if attemptFence != adapter.reservations[0].FenceToken {
			t.Fatalf("repair fence token = %q, want the reserved fence %q", attemptFence, adapter.reservations[0].FenceToken)
		}
		// The abandoning reconcile recorded the settlement and lost the
		// response: the record stays, and the ordinary path still proceeds.
		pending := h.run()
		if pending.Status.CISettlement == nil {
			t.Fatal("status.ciSettlement = nil after the lost close, want the pending settlement")
		}
		if pending.Status.CISettlement.FenceToken != attemptFence {
			t.Fatalf("pending settlement fence = %q, want the reserved fence %q", pending.Status.CISettlement.FenceToken, attemptFence)
		}
		if pending.Status.CISettlement.IdempotencyKey != adapter.reservations[0].IdempotencyKey {
			t.Fatalf("pending settlement idempotency key = %q, want the reservation key %q", pending.Status.CISettlement.IdempotencyKey, adapter.reservations[0].IdempotencyKey)
		}
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the lost settlement response = %q, want Verifying (the ending never waits on the report)", got)
		}
		// The next reconcile re-presents the settlement byte-identical — the
		// fence inside the record keys it, not the cleared park record's
		// attempt count.
		h.reconcile()
		if len(adapter.outcomes) != 2 {
			t.Fatalf("source outcomes = %#v, want the committed settlement and its replay", adapter.outcomes)
		}
		if !reflect.DeepEqual(adapter.outcomes[0], adapter.outcomes[1]) {
			t.Fatalf("replayed settlement = %#v, want byte-identical to the committed one %#v (the same fence collapses into one settled outcome)", adapter.outcomes[1], adapter.outcomes[0])
		}
		if delivered := h.run(); delivered.Status.CISettlement != nil {
			t.Fatalf("status.ciSettlement = %#v after the replay, want nil (the clear is the proof of delivery)", delivered.Status.CISettlement)
		}
	})

	// A republish across the abandon: the attempt's coordinator exited and
	// only its publication is unrecorded — the world reports the new head
	// under the abandon, with its check still pending so the ordinary path
	// settles nothing in the same reconcile. The mapped outcome is the
	// repushed one naming the new head, and the ordinary path then settles
	// the run's green set on its own terms.
	t.Run("repushed", func(t *testing.T) {
		adapter := &ciparkSource{}
		h, attemptFence := abandonedWithLiveFence(t, "cipark-abandon-fenced-repushed", adapter, func(t *testing.T, h *ciparkHarness) {
			// The push the abandoned attempt made, with the publication
			// record shadowing it: the head is this run's own published one
			// now, and the check is still pending.
			h.publishHead("sha-2")
			h.world.set(PRObservation{PR: "42", Head: "sha-2", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
			removeCIParkPolicy(t, h.laneName)
		})
		if attemptFence != adapter.reservations[0].FenceToken {
			t.Fatalf("repair fence token = %q, want the reserved fence %q", attemptFence, adapter.reservations[0].FenceToken)
		}
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the abandoned republish = %q, want Verifying (the pending check never settles)", got)
		}
		abandoned := h.run()
		if abandoned.Status.CISettlement != nil {
			t.Fatalf("status.ciSettlement = %#v after the abandon, want nil (the delivery cleared it)", abandoned.Status.CISettlement)
		}
		if len(adapter.outcomes) != 1 {
			t.Fatalf("source outcomes = %#v, want exactly one settled fence", adapter.outcomes)
		}
		if outcome := adapter.outcomes[0]; outcome.Result != source.CIRepairRepushed || outcome.Head != "sha-2" || outcome.FenceToken != attemptFence {
			t.Fatalf("delivered outcome = %#v, want the reserved fence settled repushed with the new head sha-2", outcome)
		}
		if abandoned.Status.CIRepair != nil || abandoned.Status.CIPark != nil || abandoned.Status.CIParkDeferral != nil || abandoned.Status.CIParkRelease != nil {
			t.Fatalf("run status after the abandon = attempt %#v park %#v deferral %#v release %#v, want all nil", abandoned.Status.CIRepair, abandoned.Status.CIPark, abandoned.Status.CIParkDeferral, abandoned.Status.CIParkRelease)
		}
		if len(adapter.releases) != 0 {
			t.Fatalf("releases = %d, want 0 (the fenced outcome is the release)", len(adapter.releases))
		}

		// The ordinary path settles the abandoned run on its own terms: two
		// consecutive green observations on the new head.
		h.world.set(greenObservation("sha-2", "build"))
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the first green observation = %q, want Verifying", got)
		}
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
			t.Fatalf("phase after the settled green observation = %q, want AwaitingReview", got)
		}
		h.reconcile()
		if len(adapter.outcomes) != 1 || len(adapter.releases) != 0 {
			t.Fatalf("source surface after the terminal settle = %d outcomes/%d releases, want 1/0", len(adapter.outcomes), len(adapter.releases))
		}
	})

	// A foreign head across the abandon: the policy was removed while the
	// observed head is one the run never published — the same failed-closed
	// classification the loop's own observation would record, and the
	// ordinary path, not the loop, owns the run from here.
	t.Run("unsupported", func(t *testing.T) {
		adapter := &ciparkSource{}
		h, attemptFence := abandonedWithLiveFence(t, "cipark-abandon-fenced-unsupported", adapter, func(t *testing.T, h *ciparkHarness) {
			// The recorded head is gone from the world: the observation
			// reports a head the run never published, so the attempt cannot
			// be graded a republish and no progress can be consumed either.
			h.world.set(PRObservation{PR: "42", Head: "sha-9", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
			removeCIParkPolicy(t, h.laneName)
		})
		if attemptFence != adapter.reservations[0].FenceToken {
			t.Fatalf("repair fence token = %q, want the reserved fence %q", attemptFence, adapter.reservations[0].FenceToken)
		}
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the abandoned foreign head = %q, want Verifying", got)
		}
		abandoned := h.run()
		if abandoned.Status.CISettlement != nil {
			t.Fatalf("status.ciSettlement = %#v after the abandon, want nil (the delivery cleared it)", abandoned.Status.CISettlement)
		}
		if len(adapter.outcomes) != 1 {
			t.Fatalf("source outcomes = %#v, want exactly one settled fence", adapter.outcomes)
		}
		if outcome := adapter.outcomes[0]; outcome.Result != source.CIRepairUnsupported || outcome.Head != "" || outcome.FenceToken != attemptFence {
			t.Fatalf("delivered outcome = %#v, want the reserved fence settled unsupported with no head", outcome)
		}
		if abandoned.Status.CIRepair != nil || abandoned.Status.CIPark != nil || abandoned.Status.CIParkDeferral != nil || abandoned.Status.CIParkRelease != nil {
			t.Fatalf("run status after the abandon = attempt %#v park %#v deferral %#v release %#v, want all nil", abandoned.Status.CIRepair, abandoned.Status.CIPark, abandoned.Status.CIParkDeferral, abandoned.Status.CIParkRelease)
		}
		if len(adapter.releases) != 0 {
			t.Fatalf("releases = %d, want 0 (the fenced outcome is the release)", len(adapter.releases))
		}
		// The delivered settlement never re-fires: a later reconcile, and a
		// controller restart, re-present nothing.
		h.reconcile()
		h.reconciler = h.newReconciler()
		h.reconcile()
		if len(adapter.outcomes) != 1 || len(adapter.releases) != 0 {
			t.Fatalf("source surface after the restart = %d outcomes/%d releases, want 1/0", len(adapter.outcomes), len(adapter.releases))
		}
	})
}

// TestEnvtestCIParkAbandonedParkClearsTheStaleAttemptContext pins the
// fence-less arm's attempt context: the attempt ended with the park, so its
// context dies in the same write that records the release intent and clears
// the park record. A stale context left behind reads as an attempt still in
// flight — and the executor renders a repair contract from it on a later
// coordinator resume — so the restart after the abandon must find no attempt
// at all.
func TestEnvtestCIParkAbandonedParkClearsTheStaleAttemptContext(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarness(t, "cipark-abandon-stale-attempt", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	if len(adapter.parks) != 1 {
		t.Fatalf("park notices = %#v, want exactly one delivered notice", adapter.parks)
	}
	// A stale attempt context: it names the parked head and the attempt it
	// belongs to, and it carries no fence — no reservation was granted, so
	// the fence-less arm owns the abandon.
	stale := h.run()
	stale.Status.CIRepair = &courierv1alpha1.CIRepairContext{
		Head:         "sha-1",
		Attempt:      1,
		FailedChecks: []string{"build"},
		MaxAttempts:  2,
	}
	if err := envtestClient.Status().Update(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	removeCIParkPolicy(t, h.laneName)
	h.reconcile()
	abandoned := h.run()
	if abandoned.Status.CIRepair != nil {
		t.Fatalf("status.ciRepair = %#v after the abandon, want nil (the attempt ended with the park)", abandoned.Status.CIRepair)
	}
	if abandoned.Status.CIPark != nil {
		t.Fatalf("status.cipark = %#v after the abandon, want nil (the record's absence fences a re-conversion)", abandoned.Status.CIPark)
	}
	// The fence-less close is still what the park owed the source.
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close for the abandoned park", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != adapter.parks[0].IdempotencyKey {
		t.Fatalf("close key = %q, want the park notice's own key %q", adapter.releases[0].IdempotencyKey, adapter.parks[0].IdempotencyKey)
	}
	// A restart reads the same cleared context: the run carries no attempt a
	// resumed coordinator could render a repair contract from.
	h.reconciler = h.newReconciler()
	h.reconcile()
	if got := h.run().Status.CIRepair; got != nil {
		t.Fatalf("status.ciRepair = %#v after the restart, want nil (no attempt is in flight)", got)
	}
}

// TestEnvtestCIParkAbandonedParkAdoptsTheLegacySettlementKey pins the
// fence-less arm's adoption of a legacy settlement key: a settlement record
// written by an older controller carries no idempotency key, because its
// delivery derived the key from the park account — and the abandon is the
// last moment that account exists. The key is adopted from the very account
// the older controller would have derived it from, so the settlement is
// replayed to the source instead of being dropped as delivered, and the
// source-side reservation it answers is settled.
func TestEnvtestCIParkAbandonedParkAdoptsTheLegacySettlementKey(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &refusingSettlementSource{}
	h := newCIParkHarness(t, "cipark-abandon-legacy-key", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.settleFailure("sha-1", "build")
	// The settled failure woke the run for one fence-backed repair.
	resumed := h.run()
	if resumed.Status.CIRepair == nil || resumed.Status.CIRepair.FenceToken == "" {
		t.Fatalf("repair context after the resume = %#v, want the attempt and the fence the reservation granted", resumed.Status.CIRepair)
	}
	if resumed.Status.CIPark == nil || resumed.Status.CIPark.Attempts != 1 {
		t.Fatalf("park account = %#v, want one consumed attempt", resumed.Status.CIPark)
	}
	attemptFence := resumed.Status.CIRepair.FenceToken
	if attemptFence != adapter.reservations[0].FenceToken {
		t.Fatalf("repair fence token = %q, want the reserved fence %q", attemptFence, adapter.reservations[0].FenceToken)
	}
	// The resumed coordinator runs and exits; the phase write carries the
	// attempt context across untouched.
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the resumed coordinator exited = %q, want Verifying", got)
	}
	// An older controller wrote the settlement: the outcome is on the run and
	// the attempt is over — its context is gone — but the record carries no
	// idempotency key, because the older controller derived the key from the
	// park account at delivery time.
	legacy := h.run()
	legacy.Status.CIRepair = nil
	legacy.Status.CISettlement = &courierv1alpha1.CISettlement{
		FenceToken: attemptFence,
		Result:     courierv1alpha1.CIParkOutcomeExhausted,
	}
	if err := envtestClient.Status().Update(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	removeCIParkPolicy(t, h.laneName)
	// A calm pending world, so the ordinary path the abandon hands to settles
	// nothing in the same reconcile.
	h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
	h.reconcile()
	// The source refused the report, so the record is still pending — and the
	// park account it would be derived from is gone with the abandoned park.
	abandoned := h.run()
	if abandoned.Status.CIPark != nil {
		t.Fatalf("status.cipark = %#v after the abandon, want nil (the record's absence fences a re-conversion)", abandoned.Status.CIPark)
	}
	pending := abandoned.Status.CISettlement
	if pending == nil {
		t.Fatal("status.ciSettlement = nil after the abandon, want the pending settlement (the source refused its report)")
	}
	wantKey := ciparkRepairKey(abandoned, 1)
	if pending.IdempotencyKey != wantKey {
		t.Fatalf("pending settlement idempotency key = %q, want the key adopted from the park account %q", pending.IdempotencyKey, wantKey)
	}
	if len(adapter.outcomes) != 0 {
		t.Fatalf("source outcomes = %#v, want none (the source refused the report)", adapter.outcomes)
	}
	// The next reconcile replays the settlement with the adopted key: the
	// record is delivered, not dropped, and the reservation it answers is
	// settled at the source.
	h.reconcile()
	if len(adapter.outcomes) != 1 {
		t.Fatalf("source outcomes = %#v, want exactly one settled fence (the settlement was replayed, not dropped)", adapter.outcomes)
	}
	outcome := adapter.outcomes[0]
	if outcome.FenceToken != attemptFence {
		t.Fatalf("delivered fence = %q, want the reserved fence %q", outcome.FenceToken, attemptFence)
	}
	if outcome.IdempotencyKey != wantKey {
		t.Fatalf("delivered idempotency key = %q, want the adopted key %q", outcome.IdempotencyKey, wantKey)
	}
	if outcome.Result != source.CIRepairExhausted {
		t.Fatalf("delivered result = %q, want %q", outcome.Result, source.CIRepairExhausted)
	}
	if got := h.run().Status.CISettlement; got != nil {
		t.Fatalf("status.ciSettlement = %#v after the delivery, want nil (the clear is the proof of delivery)", got)
	}
}

// TestEnvtestCIParkAbandonedParkSettlesAFenceWithoutAParkRecord pins the
// abandon's armed branch when the park record is gone: a run that still
// carries a granted reservation is settled by the fenced outcome anyway, so
// a reservation is never left unsettled. The park record's absence ends the
// fence-less arm only — the live fence is read first.
func TestEnvtestCIParkAbandonedParkSettlesAFenceWithoutAParkRecord(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &ciparkSource{}
	h := newCIParkHarness(t, "cipark-abandon-fence-no-park", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	// The park record is gone, but the attempt's granted fence is not: the
	// reservation the source holds is still outstanding.
	fence := ciparkFenceToken(parked, 1)
	parked.Status.CIPark = nil
	parked.Status.CIRepair = &courierv1alpha1.CIRepairContext{
		Head:        "sha-1",
		Attempt:     1,
		FenceToken:  fence,
		MaxAttempts: 2,
	}
	if err := envtestClient.Status().Update(context.Background(), parked); err != nil {
		t.Fatal(err)
	}
	removeCIParkPolicy(t, h.laneName)
	h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
	h.reconcile()
	abandoned := h.run()
	if abandoned.Status.CIRepair != nil {
		t.Fatalf("status.ciRepair = %#v after the abandon, want nil (the reservation is settled)", abandoned.Status.CIRepair)
	}
	if len(adapter.outcomes) != 1 {
		t.Fatalf("source outcomes = %#v, want exactly one settled fence", adapter.outcomes)
	}
	outcome := adapter.outcomes[0]
	if outcome.FenceToken != fence {
		t.Fatalf("delivered fence = %q, want the granted fence %q", outcome.FenceToken, fence)
	}
	if outcome.IdempotencyKey != ciparkRepairKey(abandoned, 1) {
		t.Fatalf("delivered idempotency key = %q, want the attempt's own key %q", outcome.IdempotencyKey, ciparkRepairKey(abandoned, 1))
	}
	if outcome.Result != source.CIRepairExhausted {
		t.Fatalf("delivered result = %q, want %q", outcome.Result, source.CIRepairExhausted)
	}
	if got := abandoned.Status.CISettlement; got != nil {
		t.Fatalf("status.ciSettlement = %#v after the delivery, want nil (the clear is the proof of delivery)", got)
	}
	// A granted reservation is never released fence-less: the fenced outcome
	// is the release.
	if len(adapter.releases) != 0 {
		t.Fatalf("releases = %d, want 0 (the fenced outcome releases the deferral)", len(adapter.releases))
	}
}

// TestEnvtestCIParkAbandonLookupFailureKeepsTheConversionAtomic pins the
// abandon's own lookup boundary (#553), mirroring the green settle's: a
// source lookup that cannot answer the capability question ends nothing —
// the park, the pending notice, the release record, and the fenced
// settlement are all unchanged, no half-conversion is persisted — and a
// later abandon with a working lookup converts the notice into the release
// exactly once.
func TestEnvtestCIParkAbandonLookupFailureKeepsTheConversionAtomic(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingNoticeSource{}
	h := newCIParkHarness(t, "cipark-abandon-lookup-fails", ciparkPolicy(2), adapter)
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	parked := h.run()
	if parked.Status.CIParkDeferral == nil {
		t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
	}
	pendingKey := parked.Status.CIParkDeferral.IdempotencyKey
	removeCIParkPolicy(t, h.laneName)
	// A reconciler whose registry no longer names the run's source is how a
	// source lookup failure reaches the abandon.
	broken := h.newReconciler()
	broken.Sources = NewSourceRegistry(map[string]source.Adapter{})
	if _, err := broken.abandonCIPark(context.Background(), h.run(), PRObservation{PR: "42", Head: "sha-1"}); err == nil {
		t.Fatal("abandonCIPark with an unresolvable source = nil error, want the lookup error")
	}
	// Nothing was persisted: the park and the pending notice are untouched,
	// no release or settlement record exists, and the run is not ended.
	half := h.run()
	if half.Status.CIPark == nil {
		t.Fatal("status.cipark = nil after the failed lookup, want the park record (no half-conversion)")
	}
	if half.Status.CIParkDeferral == nil || half.Status.CIParkDeferral.IdempotencyKey != pendingKey {
		t.Fatalf("status.ciparkDeferral = %#v after the failed lookup, want the pending notice unchanged (no half-conversion)", half.Status.CIParkDeferral)
	}
	if half.Status.CIParkRelease != nil || half.Status.CISettlement != nil {
		t.Fatalf("records after the failed lookup = release %#v settlement %#v, want neither (no record without the lookup)", half.Status.CIParkRelease, half.Status.CISettlement)
	}
	if half.Status.Phase != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the failed lookup = %q, want Verifying (the abandon did not end the run)", half.Status.Phase)
	}
	// With the lookup working again, the abandon converts the notice into
	// the release exactly once, and the close carries the notice's key.
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after the retried abandon = %q, want Verifying (the ordinary path keeps the pending world)", got)
	}
	converted := h.run()
	if converted.Status.CIPark != nil {
		t.Fatalf("status.cipark = %#v after the retried abandon, want nil (the record's absence fences a re-conversion)", converted.Status.CIPark)
	}
	if converted.Status.CIParkDeferral != nil {
		t.Fatalf("status.ciparkDeferral = %#v after the retried abandon, want nil (converted into the release)", converted.Status.CIParkDeferral)
	}
	if converted.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v at the abandon, want nil (the delivery cleared it)", converted.Status.CIParkRelease)
	}
	if len(adapter.releases) != 1 {
		t.Fatalf("releases = %#v, want exactly one close keyed by the pending notice", adapter.releases)
	}
	if adapter.releases[0].IdempotencyKey != pendingKey {
		t.Fatalf("release key = %q, want the pending notice's own key %q", adapter.releases[0].IdempotencyKey, pendingKey)
	}
}

// TestEnvtestCIParkMergedBeforeTheLoopSettlesTheDeferral pins the merged
// branch's own conversion (#555): the merged check in observeVerifying runs
// before the park loop, so a parked run whose pull request is reported merged
// terminalizes Done on a path every loop bail-out conversion was written to
// guard. The abandon runs there exactly as it does on the bail-outs — the
// accepted deferral is closed fence-less, a notice the source never accepted
// is converted into the advisory close without fabricating a settlement, and
// a live repair fence settles its reservation instead of stranding it against
// a run that is already Done.
func TestEnvtestCIParkMergedBeforeTheLoopSettlesTheDeferral(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}

	t.Run("accepted-deferral", func(t *testing.T) {
		adapter := &ciparkSource{}
		h := newCIParkHarness(t, "cipark-merged-accepted", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		if len(adapter.parks) != 1 {
			t.Fatalf("park notices = %#v, want exactly one delivered notice", adapter.parks)
		}
		if h.run().Status.CIParkDeferral != nil {
			t.Fatal("status.ciparkDeferral = non-nil after the accepted notice, want nil")
		}
		// The merge arrives while the run is parked: the merged branch owns
		// the observation before the loop ever sees it.
		h.world.set(PRObservation{PR: "42", Head: "sha-1", Merged: true})
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseDone {
			t.Fatalf("phase after the merged observation = %q, want Done", got)
		}
		// The reconcile after the terminal transition resolves the source
		// work and starts the retention window.
		h.reconcile()
		done := h.run()
		if _, ok := doneAtMarker(done); !ok {
			t.Fatalf("done-at marker = %#v, want the retention window started on the resolved run", done.Annotations)
		}
		// The fence-less close went out keyed by the park notice's own
		// identity, and the delivery cleared the record that carried it.
		if len(adapter.releases) != 1 {
			t.Fatalf("releases = %#v, want exactly one close for the parked run's deferral", adapter.releases)
		}
		if adapter.releases[0].IdempotencyKey != adapter.parks[0].IdempotencyKey {
			t.Fatalf("close key = %q, want the park notice's own key %q", adapter.releases[0].IdempotencyKey, adapter.parks[0].IdempotencyKey)
		}
		if head := adapter.releases[0].Head; head != "sha-1" {
			t.Fatalf("close head = %q, want the parked head sha-1", head)
		}
		if done.Status.CIPark != nil || done.Status.CIParkDeferral != nil || done.Status.CIParkRelease != nil || done.Status.CIRepair != nil {
			t.Fatalf("run status at Done = park %#v deferral %#v release %#v attempt %#v, want all nil", done.Status.CIPark, done.Status.CIParkDeferral, done.Status.CIParkRelease, done.Status.CIRepair)
		}
		if done.Status.CISettlement != nil {
			t.Fatalf("status.ciSettlement = %#v at Done, want nil (no reservation was ever granted)", done.Status.CISettlement)
		}
		// The terminal run re-presents nothing.
		h.reconcile()
		h.reconciler = h.newReconciler()
		h.reconcile()
		if len(adapter.releases) != 1 {
			t.Fatalf("releases after further reconciles = %d, want exactly one", len(adapter.releases))
		}
	})

	t.Run("pending-notice", func(t *testing.T) {
		adapter := &failingNoticeSource{}
		h := newCIParkHarness(t, "cipark-merged-pending-notice", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		parked := h.run()
		if parked.Status.CIParkDeferral == nil {
			t.Fatal("status.ciparkDeferral = nil while the notice keeps failing, want the pending notice")
		}
		pendingKey := parked.Status.CIParkDeferral.IdempotencyKey
		h.world.set(PRObservation{PR: "42", Head: "sha-1", Merged: true})
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseDone {
			t.Fatalf("phase after the merged observation = %q, want Done", got)
		}
		done := h.run()
		if done.Status.CIPark != nil || done.Status.CIParkDeferral != nil || done.Status.CIParkRelease != nil {
			t.Fatalf("run status at Done = park %#v deferral %#v release %#v, want all nil", done.Status.CIPark, done.Status.CIParkDeferral, done.Status.CIParkRelease)
		}
		// The advisory close went out keyed by the never-accepted notice, and
		// the merged branch records nothing it does not owe: no reservation
		// was ever granted, so no settlement is fabricated.
		if len(adapter.releases) != 1 {
			t.Fatalf("releases = %#v, want exactly one advisory close even though the notice never landed", adapter.releases)
		}
		if adapter.releases[0].IdempotencyKey != pendingKey {
			t.Fatalf("close key = %q, want the pending notice's own key %q", adapter.releases[0].IdempotencyKey, pendingKey)
		}
		if len(adapter.parks) != 0 {
			t.Fatalf("park notices = %d, want 0 (the source never accepted one)", len(adapter.parks))
		}
		if len(adapter.outcomes) != 0 || done.Status.CISettlement != nil {
			t.Fatalf("settlement surface = outcomes %#v record %#v, want none (the merged branch owes no settlement)", adapter.outcomes, done.Status.CISettlement)
		}
	})

	t.Run("live-fence", func(t *testing.T) {
		adapter := &ciparkSource{}
		h := newCIParkHarness(t, "cipark-merged-live-fence", ciparkPolicy(2), adapter)
		h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
		h.settleFailure("sha-1", "build")
		// The settled failure wakes the run for one repair; the reservation
		// is fenced.
		if got := h.phase(); got != courierv1alpha1.PhaseClaimed {
			t.Fatalf("phase after the settled failure = %q, want Claimed", got)
		}
		attemptFence := h.run().Status.CIRepair.FenceToken
		if attemptFence == "" {
			t.Fatal("repair fence token = empty after the resume, want the grant the reserve returned")
		}
		// The resumed coordinator runs and exits; the fence rides the phase
		// write into Verifying untouched.
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseRunning {
			t.Fatalf("phase after the resumed launch = %q, want Running", got)
		}
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
			t.Fatalf("phase after the resumed coordinator exited = %q, want Verifying", got)
		}
		// The merge arrives while the fence is still live.
		h.world.set(PRObservation{PR: "42", Head: "sha-1", Merged: true})
		h.reconcile()
		if got := h.phase(); got != courierv1alpha1.PhaseDone {
			t.Fatalf("phase after the merged observation = %q, want Done", got)
		}
		done := h.run()
		// The armed branch settled the reservation: the mapped no-progress
		// outcome rides the fence, and the record's absence is the proof of
		// delivery. No fence-less close is recorded on its behalf.
		if len(adapter.outcomes) != 1 {
			t.Fatalf("source outcomes = %#v, want exactly one settled fence", adapter.outcomes)
		}
		if outcome := adapter.outcomes[0]; outcome.Result != source.CIRepairExhausted || outcome.FenceToken != attemptFence || outcome.Head != "" {
			t.Fatalf("delivered outcome = %#v, want the reserved fence settled exhausted with no head", outcome)
		}
		if len(adapter.releases) != 0 {
			t.Fatalf("releases = %d, want 0 (the fenced outcome releases the deferral)", len(adapter.releases))
		}
		if done.Status.CIPark != nil || done.Status.CIParkDeferral != nil || done.Status.CIParkRelease != nil || done.Status.CIRepair != nil || done.Status.CISettlement != nil {
			t.Fatalf("run status at Done = park %#v deferral %#v release %#v attempt %#v settlement %#v, want all nil", done.Status.CIPark, done.Status.CIParkDeferral, done.Status.CIParkRelease, done.Status.CIRepair, done.Status.CISettlement)
		}
	})
}

// TestEnvtestCIParkWithoutPolicyKeepsTheOrdinaryPath pins the opt-in
// boundary: a lane that declares no repair policy leaves the run on the
// ordinary post-publication grading path, with no park record and no
// repair.
func TestEnvtestCIParkWithoutPolicyKeepsTheOrdinaryPath(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	name := "cipark-no-policy"
	h := newCIParkHarness(t, name, nil, &admissionSource{})
	h.publishHead("sha-1")
	h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
	h.toVerifying()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase on a lane without a repair policy = %q, want Verifying", got)
	}
	if run := h.run(); run.Status.CIPark != nil {
		t.Fatalf("status.cipark = %#v on a lane without a repair policy, want no park record", run.Status.CIPark)
	}
	// A red check on the ordinary path hands the run off, and it never wakes
	// a coordinator for it.
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after a red check without a repair policy = %q, want HandedOff", got)
	}
	if got := h.launchCount(name); got != 1 {
		t.Fatalf("launch count = %d, want 1: the ordinary path never wakes a coordinator", got)
	}
	if run := h.run(); run.Status.CIPark != nil {
		t.Fatalf("status.cipark = %#v on the ordinary path, want no park record", run.Status.CIPark)
	}
}

// TestEnvtestCIParkFlappingFailureNeverWakes pins the settle rule: a failure
// that flaps red → pending → red is not two consecutive identical failed
// observations, so it never settles and never wakes a coordinator.
func TestEnvtestCIParkFlappingFailureNeverWakes(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	name := "cipark-flapping"
	h := newCIParkHarness(t, name, ciparkPolicy(2), &admissionSource{})
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
	h.reconcile()
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase after a flapping failure = %q, want Verifying", got)
	}
	if got := h.launchCount(name); got != 1 {
		t.Fatalf("launch count = %d, want 1: a flapping failure never wakes the coordinator", got)
	}
	run := h.run()
	if run.Status.CIPark == nil || run.Status.CIPark.Attempts != 0 {
		t.Fatalf("park record = %#v after a flapping failure, want no consumed attempt", run.Status.CIPark)
	}
}

// TestEnvtestCIParkWindowBoundsTheWaitForCapacity pins the park window over
// every wait, not only the hold: a run whose lane has no free slot hands off
// when the window closes instead of waiting for a slot forever.
func TestEnvtestCIParkWindowBoundsTheWaitForCapacity(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	name := "cipark-full-lane"
	policy := ciparkPolicy(2)
	policy.ObservationWindowSeconds = 60
	h := newCIParkHarness(t, name, policy, &admissionSource{})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
	// Another run takes the lane's only slot while this one waits.
	holder := envtestRun(name+"-holder", h.laneName)
	if err := envtestClient.Create(context.Background(), holder); err != nil {
		t.Fatal(err)
	}
	holder.Status.Phase = courierv1alpha1.PhaseRunning
	if err := envtestClient.Status().Update(context.Background(), holder); err != nil {
		t.Fatal(err)
	}
	h.world.set(redObservation("sha-1", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseVerifying {
		t.Fatalf("phase while the lane is full = %q, want Verifying", got)
	}
	if got := h.launchCount(name); got != 1 {
		t.Fatalf("launch count = %d, want 1: a full lane never wakes the coordinator", got)
	}
	// The window closes while the lane is still full.
	now = now.Add(2 * time.Minute)
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the park window closed = %q, want HandedOff", got)
	}
	run := h.run()
	if run.Status.CIPark == nil || run.Status.CIPark.LastOutcome != courierv1alpha1.CIParkOutcomeUnsupported {
		t.Fatalf("park record = %#v, want the unsupported outcome", run.Status.CIPark)
	}
}

// failingSettlementSource models a source that answers the park close report
// with an ordinary — retryable, never-permanent — error: the close stays owed
// on the run, so the operator keeps re-presenting it. The refusal is
// deliberately not source.ErrUnsupportedCIPark, which is permanent and drops
// the pending record instead of retrying it. Flipping accepted lets the close
// land, so a scenario can watch the owed close reach the source.
type failingSettlementSource struct {
	ciparkSource
	accepted bool
}

func (s *failingSettlementSource) ReportCIRepair(_ context.Context, item source.WorkItem, outcome source.CIRepairOutcome) error {
	s.items = append(s.items, item)
	s.outcomes = append(s.outcomes, outcome)
	if s.accepted {
		return nil
	}
	return errors.New("dispatch server: unavailable")
}

// stubbornCloseSource keeps both park close reports refused: the fenced
// settlement fails like failingSettlementSource, and the fence-less deferral
// release fails with an ordinary retryable error until releaseAccepted lets
// it land. A scenario can pin the postures the two close reports share while
// each is the only one owed.
type stubbornCloseSource struct {
	failingSettlementSource
	releaseAccepted bool
}

func (s *stubbornCloseSource) ReleaseCIPark(_ context.Context, _ source.WorkItem, release source.CIParkRelease) error {
	s.releases = append(s.releases, release)
	if s.releaseAccepted {
		return nil
	}
	return errors.New("dispatch server: unavailable")
}

// reviewPullRequest is the pull request the harness's merge observation reads:
// the run's own recorded identity, in the state the scenario needs.
func reviewPullRequest(state string, merged bool) forge.PullRequest {
	return forge.PullRequest{
		Number:   42,
		Repo:     "acme/widgets",
		BaseRepo: "acme/widgets",
		HeadRepo: "acme/widgets",
		HeadRef:  "courier/acme/widgets/issue-14",
		HeadSHA:  "sha-2",
		State:    state,
		Merged:   merged,
	}
}

// TestEnvtestCIParkPendingCloseNeverPreemptsLifecycleReport pins the first
// clause of the terminal posture: a park close the source keeps refusing
// never stands in front of a terminal run's pending lifecycle report. The
// report is retried on the very reconcile that re-presents the close.
func TestEnvtestCIParkPendingCloseNeverPreemptsLifecycleReport(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingSettlementSource{}
	// The source never accepts the in-review lifecycle either, so the run
	// keeps a pending report to retry.
	adapter.reportErr = errors.New("dispatch server: unavailable")
	h := newCIParkHarness(t, "cipark-close-lifecycle", ciparkPolicy(2), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.closeOwedReview()

	if !lifecycleReportPending(h.run()) {
		t.Fatal("lifecycleReported is not pending, want the pending in-review report")
	}

	reportsBefore := len(adapter.reports)
	closesBefore := len(adapter.outcomes)
	h.reconcileResult()
	if len(adapter.reports) != reportsBefore+1 {
		t.Fatalf("source lifecycle reports = %d, want the pending report retried (%d before)", len(adapter.reports), reportsBefore)
	}
	if len(adapter.outcomes) != closesBefore+1 {
		t.Fatalf("source close reports = %d, want the close re-presented (%d before)", len(adapter.outcomes), closesBefore)
	}
	if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase after the report retry = %q, want AwaitingReview", got)
	}
	if h.run().Status.CISettlement == nil {
		t.Fatal("status.ciSettlement = nil, want the pending close to stay owed")
	}
	// The wake interval is not asserted here: the report retry and the close
	// re-presentation share one 30s cadence, so the value cannot
	// discriminate. The cadence scenario pins the cap against the
	// observation's uncapped five-minute wake.
}

// TestEnvtestCIParkPendingCloseNeverPreemptsMergeObservation pins the second
// clause: a park close the source keeps refusing never stands in front of a
// terminal run's merge observation. The recorded pull request is still read,
// and a confirmed merge still finishes the run.
// undeliverableReleaseSource models a Dispatch deployment that accepted the
// park notice (a pre-existing route) but can never take the fence-less
// release: the close exchange returns the given permanent refusal — the
// connector's reclassified ErrUnsupportedCIPark for a route-absent 404, or
// the choke point's ErrPermanentRefusal for a 400/403 an identical retry
// can never clear — and the advisory posture drops the pending release. The
// source never records a close. The wrapper also counts the source
// resolves, so a scenario can prove the run resolved its work with the
// close permanently undeliverable.
type undeliverableReleaseSource struct {
	refusingParkSource
	refusal  error
	resolves int
}

func (s *undeliverableReleaseSource) ReleaseCIPark(_ context.Context, item source.WorkItem, release source.CIParkRelease) error {
	if s.refusal != nil {
		return s.refusal
	}
	return s.refusingParkSource.ReleaseCIPark(context.Background(), item, release)
}

func (s *undeliverableReleaseSource) Resolve(ctx context.Context, item source.WorkItem) error {
	s.resolves++
	return s.admissionSource.Resolve(ctx, item)
}

// TestEnvtestCIParkOlderDispatchReleaseNeverWedgesTheTerminalLifecycle pins
// the release's advisory posture under version skew: a source that accepted
// the park notice but can never take the release keeps refusing the close
// permanently, the pending release drops instead of retrying forever, and
// the run still walks its full terminal lifecycle — AwaitingReview observes
// the merged pull request, transitions to Done, and resolves the source
// work. An older source can hold a deferral; it can never hold Courier's
// completion. Both never-deliverable refusals are pinned: the reclassified
// route-absent 404 and the choke point's 400/403 permanent refusal.
func TestEnvtestCIParkOlderDispatchReleaseNeverWedgesTheTerminalLifecycle(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	for _, tc := range []struct {
		name    string
		refusal error
	}{
		{"route-absent-404", source.ErrUnsupportedCIPark},
		{"body-rejected-400", source.ErrPermanentRefusal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &undeliverableReleaseSource{
				refusingParkSource: refusingParkSource{releaseRefused: true},
				refusal:            tc.refusal,
			}
			h := newCIParkHarness(t, "cipark-old-dispatch-"+tc.name, ciparkPolicy(2), adapter)
			reader := &fakePullRequestReader{pull: reviewPullRequest("closed", true)}
			h.usePullRequests(reader)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			h.reconciler.Now = func() time.Time { return now }
			h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})
			h.world.set(greenObservation("sha-1", "build"))
			h.reconcile()
			h.reconcile()
			if got := h.phase(); got != courierv1alpha1.PhaseAwaitingReview {
				t.Fatalf("phase after the settled green observation = %q, want AwaitingReview (the ending never waits on the close)", got)
			}
			ended := h.run()
			if ended.Status.CIParkDeferral != nil {
				t.Fatalf("status.ciparkDeferral = %#v at the green settle, want nil", ended.Status.CIParkDeferral)
			}
			if ended.Status.CIParkRelease != nil {
				t.Fatalf("status.ciparkRelease = %#v after the permanent refusal, want nil (the advisory release dropped it)", ended.Status.CIParkRelease)
			}
			if len(adapter.releases) != 0 {
				t.Fatalf("releases = %#v, want 0 (a never-deliverable close is never recorded)", adapter.releases)
			}
			if !lifecycleReportSettled(h.run()) {
				t.Fatal("lifecycleReported is not settled, want the published in-review report")
			}

			// The merged pull request ends the work, and the Done lifecycle
			// resolves the source with the close permanently undeliverable.
			h.reconcileResult()
			if got := h.phase(); got != courierv1alpha1.PhaseDone {
				t.Fatalf("phase after the confirmed merge = %q, want Done", got)
			}
			h.reconcileResult()
			if adapter.resolves != 1 {
				t.Fatalf("source resolves = %d, want exactly 1 (the run resolved with the close permanently undeliverable)", adapter.resolves)
			}
			resolved := h.run()
			if _, ok := doneAtMarker(resolved); !ok {
				t.Fatalf("done-at marker = %#v, want the retention window started on the resolved run", resolved.Annotations)
			}
			if resolved.Status.CIParkRelease != nil {
				t.Fatalf("status.ciparkRelease = %#v on the resolved run, want nil", resolved.Status.CIParkRelease)
			}
			// A resolved run re-presents nothing.
			h.reconcile()
			if adapter.resolves != 1 {
				t.Fatalf("source resolves after a further reconcile = %d, want still 1", adapter.resolves)
			}
		})
	}
}

func TestEnvtestCIParkPendingCloseNeverPreemptsMergeObservation(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingSettlementSource{}
	h := newCIParkHarness(t, "cipark-close-observation", ciparkPolicy(2), adapter)
	reader := &fakePullRequestReader{pull: reviewPullRequest("closed", true)}
	h.usePullRequests(reader)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.closeOwedReview()

	// The source accepted the in-review lifecycle, so the merge observation
	// may read the recorded pull request.
	if !lifecycleReportSettled(h.run()) {
		t.Fatal("lifecycleReported is not settled, want the published in-review report")
	}

	result := h.reconcileResult()
	if reader.calls != 1 {
		t.Fatalf("pull-request reads = %d, want exactly one exact read", reader.calls)
	}
	if len(reader.requests) != 1 || reader.requests[0] != (forge.PullRequestRef{Repo: "acme/widgets", Number: 42}) {
		t.Fatalf("reads = %#v, want one exact read of PR 42 in the run's base repository", reader.requests)
	}
	if got := h.phase(); got != courierv1alpha1.PhaseDone {
		t.Fatalf("phase after the confirmed merge = %q, want Done", got)
	}
	if result.RequeueAfter != ciparkSettlementRetryDelay {
		t.Fatalf("RequeueAfter = %v, want the settlement cadence %v", result.RequeueAfter, ciparkSettlementRetryDelay)
	}
	// The close is still owed: a Done run keeps re-presenting it instead of
	// resolving and reaping the source work with the close outstanding.
	if h.run().Status.CISettlement == nil {
		t.Fatal("status.ciSettlement = nil, want the pending close to stay owed")
	}
}

// TestEnvtestCIParkPendingCloseKeepsTheSlowObservationCadence pins the
// cadence: the close retry wakes the reconcile every settlement interval, but
// the merge observation keeps its own slow cadence under a durable marker, so
// the forge is not sampled faster than the observation allows.
func TestEnvtestCIParkPendingCloseKeepsTheSlowObservationCadence(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingSettlementSource{}
	h := newCIParkHarness(t, "cipark-close-cadence", ciparkPolicy(2), adapter)
	reader := &fakePullRequestReader{pull: reviewPullRequest("open", false)}
	h.usePullRequests(reader)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.closeOwedReview()

	closesBefore := len(adapter.outcomes)
	first := h.reconcileResult()
	if reader.calls != 1 {
		t.Fatalf("pull-request reads = %d, want exactly one observation", reader.calls)
	}
	if len(adapter.outcomes) != closesBefore+1 {
		t.Fatalf("source close reports = %d, want the close re-presented on the reconcile (%d before)", len(adapter.outcomes), closesBefore)
	}
	if first.RequeueAfter != ciparkSettlementRetryDelay {
		t.Fatalf("first RequeueAfter = %v, want the settlement cadence cap %v", first.RequeueAfter, ciparkSettlementRetryDelay)
	}

	// The next reconcile comes well inside the slow cadence. The close is
	// re-presented, but the observation must not sample the forge again.
	now = now.Add(20 * time.Second)
	second := h.reconcileResult()
	if reader.calls != 1 {
		t.Fatalf("pull-request reads = %d, want the slow observation cadence to hold", reader.calls)
	}
	if len(adapter.outcomes) != closesBefore+2 {
		t.Fatalf("source close reports = %d, want the close re-presented on every reconcile", len(adapter.outcomes))
	}
	if second.RequeueAfter != ciparkSettlementRetryDelay {
		t.Fatalf("second RequeueAfter = %v, want the settlement cadence cap %v", second.RequeueAfter, ciparkSettlementRetryDelay)
	}

	// The marker is durable and the observation is due again once the slow
	// cadence has passed.
	now = now.Add(mergeObservationRequeueDelay)
	h.reconcileResult()
	if reader.calls != 2 {
		t.Fatalf("pull-request reads = %d, want the observation to be due again", reader.calls)
	}
}

// TestEnvtestCIParkOwedCloseIsNeverReapedAway pins the retention gate: a
// terminal run that still owes the source a park close is not reaped while
// the close is outstanding, because deleting the run would drop the owed
// close with the object. Its retention window starts only once the close has
// landed, so the gate never wedges the run forever.
func TestEnvtestCIParkOwedCloseIsNeverReapedAway(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingSettlementSource{}
	h := newCIParkHarness(t, "cipark-close-reap", ciparkPolicy(2), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	// A short retention and no audit floor, so the retention window is the
	// only thing that decides the run's life.
	h.reconciler.FailedRetention = time.Minute
	h.reconciler.TerminalAuditKeep = -1
	h.closeOwedReview()

	// The run terminalizes with its close outstanding: the pending settlement
	// is the owed close the source keeps refusing.
	terminal := h.run()
	terminal.Status.Phase = courierv1alpha1.PhaseFailed
	if err := envtestClient.Status().Update(context.Background(), terminal); err != nil {
		t.Fatal(err)
	}
	if !lifecycleReportSettled(h.run()) {
		t.Fatal("lifecycleReported is not settled, want the published report the reap path requires")
	}

	// The first reconcile of the terminal run: the close is re-presented, and
	// the retention window never starts while the close is owed.
	closesBefore := len(adapter.outcomes)
	h.reconcileResult()
	if !h.runExists() {
		t.Fatal("run deleted while the park close was still owed")
	}
	if len(adapter.outcomes) != closesBefore+1 {
		t.Fatalf("source close reports = %d, want the close re-presented on the reconcile (%d before)", len(adapter.outcomes), closesBefore)
	}
	if h.run().Status.CISettlement == nil {
		t.Fatal("status.ciSettlement = nil, want the pending close to stay owed")
	}

	// The retention window expires while the close is still owed: the run
	// survives, and the close is still owed.
	now = now.Add(2 * time.Minute)
	closesBefore = len(adapter.outcomes)
	result := h.reconcileResult()
	if !h.runExists() {
		t.Fatal("run deleted while the park close was still owed")
	}
	if len(adapter.outcomes) != closesBefore+1 {
		t.Fatalf("source close reports = %d, want the close re-presented on the reconcile (%d before)", len(adapter.outcomes), closesBefore)
	}
	if h.run().Status.CISettlement == nil {
		t.Fatal("status.ciSettlement = nil, want the pending close to stay owed")
	}
	if result.RequeueAfter != ciparkSettlementRetryDelay {
		t.Fatalf("RequeueAfter = %v, want the settlement cadence %v", result.RequeueAfter, ciparkSettlementRetryDelay)
	}

	// The source accepts the close: the record clears, and the retention
	// window starts from that moment.
	adapter.accepted = true
	closesBefore = len(adapter.outcomes)
	h.reconcileResult()
	if len(adapter.outcomes) != closesBefore+1 {
		t.Fatalf("source close reports = %d, want the close re-presented on the reconcile (%d before)", len(adapter.outcomes), closesBefore)
	}
	delivered := h.run()
	if delivered.Status.CISettlement != nil {
		t.Fatalf("status.ciSettlement = %#v after the source accepted the close, want nil", delivered.Status.CISettlement)
	}
	if !h.runExists() {
		t.Fatal("run deleted by the first reap-eligible reconcile; the retention window must start instead")
	}

	// Past the fresh window the run is reaped on the normal schedule: the
	// gate held it only while the close was outstanding.
	now = now.Add(2 * time.Minute)
	h.reconcileResult()
	if h.runExists() {
		t.Fatal("run still present past the retention window, want the reaped run")
	}
}

// TestEnvtestCIParkOwedReleaseIsNeverReapedAway pins the same retention gate
// for the fence-less deferral close: a terminal run that owes only a
// CIParkRelease — the shape #553's abandon leaves behind — is not reaped while
// that close is outstanding, and its retention window starts only once the
// release has landed.
func TestEnvtestCIParkOwedReleaseIsNeverReapedAway(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &stubbornCloseSource{}
	h := newCIParkHarness(t, "cipark-release-reap", ciparkPolicy(2), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.reconciler.FailedRetention = time.Minute
	h.reconciler.TerminalAuditKeep = -1
	h.closeOwedReview()

	// The run terminalizes owing only the fence-less release: the owed fenced
	// settlement the harness leaves behind is replaced by the release record
	// an abandon would have recorded, keyed by the park notice's own key.
	noticeKey := adapter.parks[0].IdempotencyKey
	terminal := h.run()
	terminal.Status.Phase = courierv1alpha1.PhaseFailed
	terminal.Status.CISettlement = nil
	terminal.Status.CIParkRelease = &courierv1alpha1.CIParkRelease{Head: "sha-2", IdempotencyKey: noticeKey}
	if err := envtestClient.Status().Update(context.Background(), terminal); err != nil {
		t.Fatal(err)
	}
	if !lifecycleReportSettled(h.run()) {
		t.Fatal("lifecycleReported is not settled, want the published report the reap path requires")
	}

	// The first reconcile of the terminal run: the release is re-presented,
	// and the retention window never starts while the close is owed.
	releasesBefore := len(adapter.releases)
	result := h.reconcileResult()
	if !h.runExists() {
		t.Fatal("run deleted while the deferral release was still owed")
	}
	if len(adapter.releases) != releasesBefore+1 {
		t.Fatalf("source deferral releases = %d, want the release re-presented on the reconcile (%d before)", len(adapter.releases), releasesBefore)
	}
	if h.run().Status.CIParkRelease == nil {
		t.Fatal("status.ciparkRelease = nil, want the pending release to stay owed")
	}
	if result.RequeueAfter != ciparkSettlementRetryDelay {
		t.Fatalf("RequeueAfter = %v, want the settlement cadence %v", result.RequeueAfter, ciparkSettlementRetryDelay)
	}

	// The retention window expires while the release is still owed: the run
	// survives, and the release is still owed.
	now = now.Add(2 * time.Minute)
	releasesBefore = len(adapter.releases)
	h.reconcileResult()
	if !h.runExists() {
		t.Fatal("run deleted while the deferral release was still owed")
	}
	if len(adapter.releases) != releasesBefore+1 {
		t.Fatalf("source deferral releases = %d, want the release re-presented on the reconcile (%d before)", len(adapter.releases), releasesBefore)
	}

	// The source accepts the release: the record clears, and the retention
	// window starts from that moment.
	adapter.releaseAccepted = true
	h.reconcileResult()
	delivered := h.run()
	if delivered.Status.CIParkRelease != nil {
		t.Fatalf("status.ciparkRelease = %#v after the source accepted the release, want nil", delivered.Status.CIParkRelease)
	}
	if !h.runExists() {
		t.Fatal("run deleted by the first reap-eligible reconcile; the retention window must start instead")
	}

	// Past the fresh window the run is reaped on the normal schedule: the
	// gate held it only while the release was outstanding.
	now = now.Add(2 * time.Minute)
	h.reconcileResult()
	if h.runExists() {
		t.Fatal("run still present past the retention window, want the reaped run")
	}
}

// TestEnvtestCIParkPendingCloseKeepsItsBoundWhenTheTerminalDispatchFails pins
// the bound on the failure path: a terminal dispatch that fails must not hand
// the wake to the workqueue's rate-limited backoff, which grows past the
// settlement cadence as the failures repeat. The close owns the wake while it
// is owed, so the reconcile asks for the settlement cadence and reports no
// error, and the close stays owed.
func TestEnvtestCIParkPendingCloseKeepsItsBoundWhenTheTerminalDispatchFails(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	adapter := &failingSettlementSource{}
	// The source never accepts the in-review lifecycle either, so the run
	// keeps a pending report to retry.
	adapter.reportErr = errors.New("dispatch server: unavailable")
	h := newCIParkHarness(t, "cipark-close-error-bound", ciparkPolicy(2), adapter)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.reconciler.Now = func() time.Time { return now }
	h.closeOwedReview()
	if !lifecycleReportPending(h.run()) {
		t.Fatal("lifecycleReported is not pending, want the pending in-review report")
	}

	// The operator's source adapter is gone: the run's source no longer
	// resolves, so the close cannot be presented and the terminal dispatch
	// fails on the same missing source.
	h.reconciler.Sources = NewSourceRegistry(map[string]source.Adapter{})

	result, err := h.reconcileOutcome()
	if err != nil {
		t.Fatalf("reconcile error = %v, want the close's bounded wake to govern the failure path", err)
	}
	if result.RequeueAfter != ciparkSettlementRetryDelay {
		t.Fatalf("RequeueAfter = %v, want the settlement cadence %v", result.RequeueAfter, ciparkSettlementRetryDelay)
	}
	if h.run().Status.CISettlement == nil {
		t.Fatal("status.ciSettlement = nil, want the pending close to stay owed")
	}
}

// TestEnvtestCIParkLegacyHeadSignalsInapplicablePolicy pins the legacy
// coordinator's signal (#550): a run on a policy-declaring lane that reaches
// Verifying with an observable pull request but no publication evidence the
// loop can attribute gets a durable condition saying the ci-repair policy
// cannot apply, and the ordinary grading path still owns the observation,
// fail-closed — no park record, no fabricated head, no repair.
func TestEnvtestCIParkLegacyHeadSignalsInapplicablePolicy(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-legacy-head", ciparkPolicy(2), &admissionSource{})

	// A legacy coordinator published its pull request without ever writing
	// the broker-verified commit record the loop attributes heads from.
	h.world.set(PRObservation{PR: "42", Head: "sha-legacy", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
	h.toVerifying()
	h.reconcile()

	run := h.run()
	if run.Status.CIPark != nil {
		t.Fatalf("park record = %#v, want none: an unattributable head never parks", run.Status.CIPark)
	}
	cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != courierv1alpha1.ConditionReasonHeadUnattributable {
		t.Fatalf("condition = %#v, want %q True with reason %q", cond, courierv1alpha1.ConditionCIRepairInapplicable, courierv1alpha1.ConditionReasonHeadUnattributable)
	}
	if cond.ObservedGeneration != run.Generation {
		t.Fatalf("condition observedGeneration = %d, want %d", cond.ObservedGeneration, run.Generation)
	}

	// The signal is idempotent across the run's own poll cadence.
	h.reconcile()
	if got := h.launchCount(h.name); got != 1 {
		t.Fatalf("launches after the idempotent poll = %d, want 1 (the ordinary path never wakes the coordinator)", got)
	}

	// The ordinary path still owns the observation, fail-closed: a settled
	// red observation hands the run off without a repair.
	h.world.set(redObservation("sha-legacy", "build"))
	h.reconcile()
	h.reconcile()
	if got := h.phase(); got != courierv1alpha1.PhaseHandedOff {
		t.Fatalf("phase after the settled red observation = %q, want HandedOff", got)
	}
	final := h.run()
	if final.Status.CIPark != nil {
		t.Fatalf("park record = %#v, want none: the ordinary path decided", final.Status.CIPark)
	}
	if got := apimeta.FindStatusCondition(final.Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable); got == nil {
		t.Fatal("condition = nil after the handoff, want the durable record of why the loop never engaged")
	}
}

// TestEnvtestCIParkAttributableHeadLeavesNoInapplicability pins the secure
// mode's silence: a run whose broker-verified publication record names the
// observed head parks as usual and never carries the inapplicability
// condition.
func TestEnvtestCIParkAttributableHeadLeavesNoInapplicability(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-attributed-head", ciparkPolicy(2), &admissionSource{})

	h.park("sha-1", CheckObservation{Name: "build", State: CheckStatePending})

	run := h.run()
	if cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable); cond != nil {
		t.Fatalf("condition = %#v, want none on an attributable head", cond)
	}
}

// TestEnvtestCIParkLateEvidenceClearsInapplicability pins the recovery: a
// legacy run carrying the inapplicability condition whose publication
// evidence arrives late becomes attributable, parks on its own head, and
// loses the obsolete condition in the same write that establishes the park.
func TestEnvtestCIParkLateEvidenceClearsInapplicability(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	h := newCIParkHarness(t, "cipark-late-evidence", ciparkPolicy(2), &admissionSource{})

	h.world.set(PRObservation{PR: "42", Head: "sha-1", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
	h.toVerifying()
	h.reconcile()
	if cond := apimeta.FindStatusCondition(h.run().Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable); cond == nil {
		t.Fatal("condition = nil before the evidence, want the legacy signal first")
	}

	// The broker-verified publication record arrives: the head becomes
	// attributable and the park engages normally.
	h.publishHead("sha-1")
	h.reconcile()

	run := h.run()
	if run.Status.CIPark == nil || run.Status.CIPark.Head != "sha-1" {
		t.Fatalf("park record = %#v, want a park on sha-1", run.Status.CIPark)
	}
	if cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable); cond != nil {
		t.Fatalf("condition = %#v, want none once the head is attributable", cond)
	}
}

// TestEnvtestCIParkRemovedPolicyClearsInapplicability pins the stale
// condition's end: a lane that stops declaring a repair policy leaves a run
// carrying the inapplicability condition with it removed, since the premise
// it recorded no longer holds.
func TestEnvtestCIParkRemovedPolicyClearsInapplicability(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	h := newCIParkHarness(t, "cipark-policy-removed", ciparkPolicy(2), &admissionSource{})

	h.world.set(PRObservation{PR: "42", Head: "sha-legacy", Checks: []CheckObservation{{Name: "build", State: CheckStatePending}}})
	h.toVerifying()
	h.reconcile()
	if cond := apimeta.FindStatusCondition(h.run().Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable); cond == nil {
		t.Fatal("condition = nil before the policy change, want the legacy signal first")
	}

	lane := &courierv1alpha1.LaneProfile{}
	if err := envtestClient.Get(ctx, envtestKey(h.laneName), lane); err != nil {
		t.Fatal(err)
	}
	lane.Spec.CIRepair = nil
	if err := envtestClient.Update(ctx, lane); err != nil {
		t.Fatal(err)
	}

	h.reconcile()
	if cond := apimeta.FindStatusCondition(h.run().Status.Conditions, courierv1alpha1.ConditionCIRepairInapplicable); cond != nil {
		t.Fatalf("condition = %#v, want none once the lane stopped declaring a policy", cond)
	}
}
