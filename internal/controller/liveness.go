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

// defaultMaxRestarts is the crashloop threshold: after this many infra
// relaunches a run is handed to a human instead of relaunched again.
const defaultMaxRestarts = 3

// livenessWindow returns the configured liveness window. Zero falls back to
// the package default.
func (r *CoderRunReconciler) livenessWindow() time.Duration {
	if r.LivenessWindow > 0 {
		return r.LivenessWindow
	}
	return defaultLivenessWindow
}

// maxRestarts returns the configured crashloop threshold. Zero falls back to
// the package default.
func (r *CoderRunReconciler) maxRestarts() int {
	if r.MaxRestarts > 0 {
		return r.MaxRestarts
	}
	return defaultMaxRestarts
}

// checkLiveness is the liveness backstop for Running runs: it reaps a
// coordinator whose activity heartbeat has gone quiet past the liveness
// window and relaunches it from its checkpoint. A missing heartbeat is never
// acted on — no liveness data is not evidence of a wedge — and neither is a
// fresh one.
//
// Each reap increments the crashloop counter. When the counter reaches the
// threshold the run transitions to NeedsHuman; otherwise it returns to
// Claimed so the next reconcile relaunches and resumes it. The second return
// value reports whether liveness took action, so the caller can stop further
// pod observation.
func (r *CoderRunReconciler) checkLiveness(ctx context.Context, run *courierv1alpha1.CoderRun, pods []corev1.Pod) (ctrl.Result, bool, error) {
	if run.Status.Heartbeat == nil {
		return ctrl.Result{}, false, nil
	}
	if r.clock().Sub(run.Status.Heartbeat.At.Time) <= r.livenessWindow() {
		return ctrl.Result{}, false, nil
	}

	// A freshly started pod has not had a chance to heartbeat yet. Only
	// reap if no coordinator container started within the liveness window.
	for i := range pods {
		pod := &pods[i]
		if !podBelongsToRun(pod, run) {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == "coordinator" && cs.State.Running != nil {
				if r.clock().Sub(cs.State.Running.StartedAt.Time) <= r.livenessWindow() {
					return ctrl.Result{}, false, nil
				}
			}
		}
	}

	for i := range pods {
		pod := &pods[i]
		if !podBelongsToRun(pod, run) {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, false, err
		}
	}

	before := run.DeepCopy()
	run.Status.Restarts++
	if run.Status.Restarts > r.maxRestarts() {
		// Persist the counter before the terminal transition: transitionTerminal
		// snapshots its own before after this point and would omit it.
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, true, err
		}
		result, err := r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "")
		return result, true, err
	}
	run.Status.Phase = courierv1alpha1.PhaseClaimed
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, true, err
	}
	r.emitPhaseTransition(run, courierv1alpha1.PhaseClaimed, map[string]any{"restarts": run.Status.Restarts})
	return ctrl.Result{Requeue: true}, true, nil
}
