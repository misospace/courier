package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// defaultLivenessWindow is how long a Running run may go without a heartbeat
// before it is considered wedged and reaped. Deliberately generous: a wedged
// run is one that stopped producing successful activity, not one that is slow.
const defaultLivenessWindow = 5 * time.Minute

// defaultMaxRestarts is the crashloop threshold: a run whose crashloop
// counter has reached this many infra relaunches is handed to a human on the
// next confirmed reap instead of relaunched again.
const defaultMaxRestarts = 3

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
// window is alive, so its consecutive-crashloop counter resets to 0. The
// backstop bounds a continuous streak of relaunches, not a lifetime total.
//
// A coordinator pod is fresh, and so must not be reaped, while it is not
// terminating and either it was created within the liveness window (covering
// the image-pull, container-creating, and informer-cache lag of a
// just-relaunched pod) or its coordinator container started within the
// window. A run with no coordinator pod at all is not fresh: that is the
// missing/relaunch case the crashloop counter bounds.
//
// A confirmed reap increments the crashloop counter, with one exception:
// once the counter has reached the threshold (Restarts >= maxRestarts) the
// run transitions to NeedsHuman with the counter left at the ceiling,
// instead of relaunched again. Below the ceiling the run returns to Claimed
// so the next reconcile relaunches and resumes it. The second return value
// reports whether liveness took action, so the caller can stop further pod
// observation.
func (r *CoderRunReconciler) checkLiveness(ctx context.Context, run *courierv1alpha1.CoderRun, pods []corev1.Pod) (ctrl.Result, bool, error) {
	if run.Status.Heartbeat == nil {
		return ctrl.Result{}, false, nil
	}
	if r.clock().Sub(run.Status.Heartbeat.At.Time) <= r.livenessWindow() {
		// A fresh heartbeat demonstrates liveness: the run is alive, so its
		// consecutive-crashloop counter resets. A nil heartbeat (no liveness
		// data) and a fresh pod (not yet demonstrated) never reach this
		// branch, so neither resets the counter.
		if run.Status.Restarts != 0 {
			before := run.DeepCopy()
			run.Status.Restarts = 0
			if err := r.patchStatus(ctx, before, run); err != nil {
				return ctrl.Result{}, false, err
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
			if cs.Name == "coordinator" && cs.State.Running != nil {
				if r.clock().Sub(cs.State.Running.StartedAt.Time) <= r.livenessWindow() {
					return ctrl.Result{}, false, nil
				}
			}
		}
	}

	missing := true
	deleted := false
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
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, false, err
		}
		deleted = true
	}
	if !missing && !deleted {
		// Every wedged coordinator pod is already terminating: a sibling
		// reconcile is finishing this reap. Take no further action. The
		// deletion's watch event normally wakes the relaunch, but a pod can
		// stay terminating (e.g. held by a finalizer) long enough that the
		// event never arrives, so the bounded re-observation delay below
		// covers the stuck-terminating case instead of the event alone.
		return ctrl.Result{RequeueAfter: observationRequeueDelay}, false, nil
	}

	if run.Status.Restarts >= r.maxRestarts() {
		// The ceiling is reached: hand the run to a human instead of
		// relaunching it again. The counter is already at the ceiling, so
		// it is left untouched.
		result, err := r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "")
		return result, true, err
	}
	before := run.DeepCopy()
	run.Status.Restarts++
	run.Status.Phase = courierv1alpha1.PhaseClaimed
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, true, err
	}
	r.emitPhaseTransition(run, courierv1alpha1.PhaseClaimed, map[string]any{"restarts": run.Status.Restarts})
	return ctrl.Result{Requeue: true}, true, nil
}
