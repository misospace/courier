package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

// defaultLivenessWindow is how long a Running run may go without a heartbeat
// before it is considered wedged and reaped. Deliberately generous: a wedged
// run is one that stopped producing successful activity, not one that is slow.
const defaultLivenessWindow = 5 * time.Minute

// defaultMaxRestarts is the crashloop threshold: a run whose crashloop
// counter has reached this many infra relaunches is handed to a human on the
// next confirmed reap instead of relaunched again.
const defaultMaxRestarts = 3

// podLostReason is the human-facing cause published when pod loss reaches the
// infrastructure restart ceiling: it names the accumulation, not a single
// relaunch.
const podLostReason = "coordinator pod lost repeatedly and reached the infrastructure restart ceiling"

// podLostEventReason is the human-facing cause a relaunch event carries when
// a pod-loss relaunch is charged, before the ceiling is in view: it names
// the event, not the accumulation.
const podLostEventReason = "coordinator pod lost"

// livenessWindow returns the configured liveness window. Zero falls back to
// the package default.
func (r *CoderRunReconciler) livenessWindow() time.Duration {
	if r.LivenessWindow > 0 {
		return r.LivenessWindow
	}
	return defaultLivenessWindow
}

// maxRestarts returns the configured crashloop threshold. Reaching it —
// Restarts >= maxRestarts() — transitions a run to NeedsHuman; below it the
// run relaunches and the counter increments. Zero falls back to the package
// default.
func (r *CoderRunReconciler) maxRestarts() int {
	if r.MaxRestarts > 0 {
		return r.MaxRestarts
	}
	return defaultMaxRestarts
}

// checkLiveness is the liveness backstop for Running runs: it reaps a
// coordinator whose activity heartbeat has gone quiet past the liveness
// window and relaunches it from its checkpoint. A missing heartbeat is never
// acted on — no liveness data is not evidence of a wedge — while a fresh
// heartbeat is evidence of liveness: a run producing activity within the
// window is alive, so its consecutive-crashloop counter resets to 0 —
// suspended while the run has no observable coordinator, since with the
// coordinator gone or unobservable the last heartbeat is evidence of the
// recent past, not of current liveness. The backstop bounds a continuous
// streak of relaunches, not a lifetime total.
//
// A coordinator pod is fresh, and so must not be reaped, while it is not
// terminating and either it was created within the liveness window (covering
// the image-pull, container-creating, and informer-cache lag of a
// just-relaunched pod) or its coordinator container started within the
// window. Pod absence is not decided here: a run with no pod at all is left
// to the disappearance backstop, which confirms the absence against the API
// server before it charges the same ceiling-bounded relaunch. A pod that
// exists and went silent is reaped here.
//
// A confirmed reap increments the crashloop counter, with one exception:
// once the counter has reached the threshold (Restarts >= maxRestarts) the
// run transitions to NeedsHuman with the counter left at the ceiling,
// instead of relaunched again. At the ceiling an unrecoverable coordinator —
// a terminal-phase object with no recorded termination status — is
// preserved, not deleted: the NeedsHuman hand-off keeps the only record of
// the cause, exactly as the disappearance backstop. Below the ceiling the
// run returns to Claimed so the next reconcile relaunches and resumes it.
// The second return value reports whether liveness took action, so the
// caller can stop further pod observation.
func (r *CoderRunReconciler) checkLiveness(ctx context.Context, run *courierv1alpha1.CoderRun, pods []corev1.Pod) (ctrl.Result, bool, error) {
	if run.Status.Heartbeat == nil {
		return ctrl.Result{}, false, nil
	}
	if r.clock().Sub(run.Status.Heartbeat.At.Time) <= r.livenessWindow() {
		// A fresh heartbeat demonstrates liveness: the run is alive, so its
		// consecutive-crashloop counter resets. A nil heartbeat (no liveness
		// data) and a fresh pod (not yet demonstrated) never reach this
		// branch, so neither resets the counter. A missing or unobservable
		// coordinator suspends the reset: otherwise every eviction detection
		// would reset the counter it is about to charge.
		if coordinator := coordinatorPodOf(pods, run); coordinator != nil && !coordinatorPodUnrecoverable(coordinator) {
			if run.Status.Restarts != 0 {
				before := run.DeepCopy()
				run.Status.Restarts = 0
				if err := r.patchStatus(ctx, before, run); err != nil {
					return ctrl.Result{}, false, err
				}
			}
		}
		return ctrl.Result{}, false, nil
	}

	// A just-relaunched pod has not had a chance to heartbeat yet. Its
	// coordinator may not be running at all, and may not even be in the
	// informer cache, so freshness is judged on observable pod state, not on
	// the container's running state alone. A pod already terminating is
	// never fresh.
	for i := range pods {
		pod := &pods[i]
		if !podBelongsToRun(pod, run) || pod.DeletionTimestamp != nil {
			continue
		}
		if r.clock().Sub(pod.CreationTimestamp.Time) <= r.livenessWindow() {
			return ctrl.Result{}, false, nil
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == coordinatorContainerName && cs.State.Running != nil {
				if r.clock().Sub(cs.State.Running.StartedAt.Time) <= r.livenessWindow() {
					return ctrl.Result{}, false, nil
				}
			}
		}
	}

	// The ceiling is judged once, before the loop: a pod skipped at the
	// ceiling is preserved as evidence, not deleted, so the run must still
	// terminalize in this reconcile.
	atCeiling := run.Status.Restarts >= r.maxRestarts()
	missing := true
	deleted := false
	preserved := false
	deletedUnrecoverableCoordinator := false
	for i := range pods {
		pod := &pods[i]
		if !podBelongsToRun(pod, run) {
			continue
		}
		missing = false
		// A pod already terminating is not re-deleted: its deletion is in
		// flight, and re-counting it would double the crashloop counter for
		// a single wedge.
		if pod.DeletionTimestamp != nil {
			continue
		}
		// At the restart ceiling an unrecoverable coordinator is inert
		// evidence, not a wedge: skip the delete so the NeedsHuman hand-off
		// keeps the only record of the cause, mirroring the disappearance
		// backstop. No replacement needs the name. Only the run's
		// coordinator object counts: a worker or broker pod in a terminal
		// phase records no coordinator termination, and the preserved
		// evidence is the run's coordinator.
		if atCeiling && hasCoordinatorContainer(pod) && coordinatorPodUnrecoverable(pod) {
			preserved = true
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, false, err
		}
		deleted = true
		deletedUnrecoverableCoordinator = deletedUnrecoverableCoordinator ||
			hasCoordinatorContainer(pod) && coordinatorPodUnrecoverable(pod)
	}
	if missing {
		// No pod belongs to the run at all. The absence is not decided here:
		// it is left to the disappearance backstop, which confirms it against
		// the API server with a live read and takes the same ceiling-bounded
		// relaunch.
		return ctrl.Result{}, false, nil
	}
	// A delete is not a disappearance here either: when the wedged pod was
	// an unobservable dead coordinator, the charge waits for the live read
	// to show the name free; the deferred loss is then charged by the
	// disappearance backstop, which confirms it against the API server and
	// carries its pod-loss cause.
	if deletedUnrecoverableCoordinator {
		present, err := r.coordinatorObjectPresentLive(ctx, run)
		if err != nil || present {
			return ctrl.Result{RequeueAfter: observationRequeueDelay}, true, nil
		}
	}
	if !deleted && !preserved {
		// Every wedged coordinator pod is already terminating, and none was
		// deliberately preserved at the ceiling: a sibling reconcile is
		// finishing this reap. Take no further action. The deletion's watch
		// event normally wakes the relaunch, but a pod can stay terminating
		// (e.g. held by a finalizer) long enough that the event never
		// arrives, so the bounded re-observation delay below covers the
		// stuck-terminating case instead of the event alone.
		return ctrl.Result{RequeueAfter: observationRequeueDelay}, false, nil
	}

	// A confirmed wedge is the same infrastructure loss as a vanished pod,
	// so it takes the shared relaunch path and its restart ceiling. A reaped
	// wedge carries no reason: the pod-loss cause is published only by the
	// disappearance backstop, which confirms the loss against the API
	// server, so a reaped wedge keeps the generic message in both the event
	// stream and the ceiling hand-off.
	result, err := r.relaunchAfterInfraLoss(ctx, run, "", "")
	return result, true, err
}

// coordinatorObjectPresentLive reports whether any coordinator object for the
// run still exists on the API server — a survivor means the deterministic pod
// name is not free; a failed read is not evidence the name is free (callers
// treat err as defer).
func (r *CoderRunReconciler) coordinatorObjectPresentLive(ctx context.Context, run *courierv1alpha1.CoderRun) (bool, error) {
	var pods corev1.PodList
	if err := r.reader().List(ctx, &pods,
		client.InNamespace(run.Namespace),
		client.MatchingLabels{executor.LabelRun: executor.RunLabelValue(run.Name)},
	); err != nil {
		return false, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if podBelongsToRun(pod, run) && hasCoordinatorContainer(pod) {
			return true, nil
		}
	}
	return false, nil
}

// observeMissingCoordinator is the disappearance backstop for a Running run
// whose coordinator is missing or unobservable in its cache. Pod loss —
// eviction, node drain, manual deletion — is infrastructure loss with no
// ambiguity about work in flight, so it is acted on whatever the heartbeat
// says.
//
// The cache is not trusted for that judgement, so the coordinator is looked
// for on the API server directly, with no timer and no recorded state. A
// coordinator pod found live means the cache lagged and nothing is charged.
// A terminal-phase pod with no recorded coordinator exit state is
// indistinguishable from a loss — Kubernetes never reported an exit for it,
// so no model outcome can be inferred from it — and is confirmed by the same
// live read: its inert object is deleted so the run's deterministic pod name
// is free for the replacement, except at the restart ceiling, where it
// survives as the NeedsHuman hand-off's only record of the cause, and the
// relaunch is charged as pod loss against the shared ceiling. Below the
// ceiling the charge is deferred until a live read shows the deleted object
// is gone, so the deterministic pod name is provably free before the relaunch
// is charged — one physical loss is charged exactly once, and a replacement
// can never attach to a dying object the launcher would tolerate. A
// coordinator pod whose deletion is already in flight is neither a live
// coordinator nor a confirmed loss, so the run is re-observed. A read that
// fails is not evidence of a loss either, and defers the same way. Only an
// API server with no live coordinator for the run charges the bounded
// relaunch with its restart ceiling.
func (r *CoderRunReconciler) observeMissingCoordinator(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	var pods corev1.PodList
	if err := r.reader().List(ctx, &pods,
		client.InNamespace(run.Namespace),
		client.MatchingLabels{executor.LabelRun: executor.RunLabelValue(run.Name)},
	); err != nil {
		// A read that failed is not evidence of a loss.
		return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
	}
	var dead []corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podBelongsToRun(pod, run) || !hasCoordinatorContainer(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			// A deletion already in flight is neither a live coordinator nor
			// a confirmed loss, so the run is re-observed until the object is
			// gone.
			return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
		}
		if coordinatorPodUnrecoverable(pod) {
			dead = append(dead, *pod)
			continue
		}
		// A live coordinator means the cache lagged; nothing is charged.
		return ctrl.Result{}, nil
	}
	if run.Status.Restarts < r.maxRestarts() {
		// Below the ceiling the confirmed dead object is deleted so the
		// deterministic pod name is free for the replacement. At the ceiling
		// the inert object survives: the NeedsHuman hand-off keeps the only
		// record of the cause, and no replacement needs the name.
		for i := range dead {
			if err := client.IgnoreNotFound(r.Delete(ctx, &dead[i])); err != nil {
				// A failed delete is not evidence of a loss either.
				return ctrl.Result{}, err
			}
		}
		if len(dead) > 0 {
			// A delete is not a disappearance: the deleted object can still
			// be terminating, held by a finalizer (deletionTimestamp set,
			// object persists), and the relaunch path tolerates an existing
			// object, so a replacement could attach to the dying object and
			// the same physical loss be charged a second time when it finally
			// vanishes. Re-confirm with one more live read that no
			// coordinator object for the run survives before the relaunch is
			// charged: a survivor — terminating or not — defers the charge so
			// the deterministic name is provably free first. A failed read is
			// not evidence the name is free either.
			present, err := r.coordinatorObjectPresentLive(ctx, run)
			if err != nil || present {
				return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
			}
		}
	}
	return r.relaunchAfterInfraLoss(ctx, run, podLostReason, podLostEventReason)
}

// relaunchAfterInfraLoss returns a run to Claimed so the next reconcile
// relaunches and resumes it from durable state: the work branch is adopted,
// never recreated. Reaching the crashloop ceiling hands the run to a human
// with the reason the caller supplies — the cause shown to a human —
// instead of relaunching it again, and leaves the counter at the ceiling.
// The relaunch event carries the caller's event reason when it is
// non-empty, so a pod loss and a reaped wedge are distinguishable in the
// event stream: the event reason names the event, the ceiling error names
// the accumulation.
func (r *CoderRunReconciler) relaunchAfterInfraLoss(ctx context.Context, run *courierv1alpha1.CoderRun, reason, eventReason string) (ctrl.Result, error) {
	if run.Status.Restarts >= r.maxRestarts() {
		// The ceiling is reached: hand the run to a human instead of
		// relaunching it again. The counter is already at the ceiling, so
		// it is left untouched.
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "", terminalLifecycleIntent{
			error: reason,
		}, nil)
	}
	before := run.DeepCopy()
	run.Status.Restarts++
	run.Status.Phase = courierv1alpha1.PhaseClaimed
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, err
	}
	detail := map[string]any{"restarts": run.Status.Restarts}
	if eventReason != "" {
		detail["reason"] = eventReason
	}
	r.emitPhaseTransition(run, courierv1alpha1.PhaseClaimed, detail)
	return ctrl.Result{Requeue: true}, nil
}
