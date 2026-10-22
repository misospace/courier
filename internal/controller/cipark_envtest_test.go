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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
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
	t          *testing.T
	name       string
	laneName   string
	adapter    source.Adapter
	world      *ciparkWorld
	launched   []string
	reconciler *CoderRunReconciler
}

// newCIParkHarness creates the lane, the run, and the reconciler that drives
// them. A nil policy leaves the park-and-resume loop disabled, exactly as a
// lane that never declared one does.
func newCIParkHarness(t *testing.T, name string, policy *courierv1alpha1.CIRepairPolicy, adapter source.Adapter) *ciparkHarness {
	t.Helper()
	ctx := context.Background()
	laneName := name + "-lane"
	lane := envtestLane(laneName)
	lane.Spec.CIRepair = policy
	if err := envtestClient.Create(ctx, lane); err != nil {
		t.Fatal(err)
	}
	if err := envtestClient.Create(ctx, envtestRun(name, laneName)); err != nil {
		t.Fatal(err)
	}
	h := &ciparkHarness{t: t, name: name, laneName: laneName, adapter: adapter, world: &ciparkWorld{}}
	h.reconciler = h.newReconciler()
	return h
}

// newReconciler builds a reconciler over the harness's world and launch
// function. A second instance is how a controller restart is simulated.
func (h *ciparkHarness) newReconciler() *CoderRunReconciler {
	return &CoderRunReconciler{
		Client: envtestClient,
		Sources: NewSourceRegistry(map[string]source.Adapter{
			"manual": h.adapter,
		}),
		StatusWriter: status.KubePatchWriter{Client: envtestClient},
		Observer:     h.world,
		Launch:       h.launch,
	}
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
	if _, err := h.reconciler.Reconcile(context.Background(), envtestRequest(name)); err != nil {
		h.t.Fatal(err)
	}
}

// run reads the harness's run back from the API server.
func (h *ciparkHarness) run() *courierv1alpha1.CoderRun {
	h.t.Helper()
	return h.runNamed(h.name)
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
