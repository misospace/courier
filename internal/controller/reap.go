package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// doneAtAnnotation records, as an RFC3339 timestamp, when Courier first
// observed a CoderRun in Done with its source work resolved. It is the durable
// retention clock: the creation timestamp is useless here because a run can
// exist far longer than the retention window before it succeeds, and process
// memory dies with the controller, so the marker must live on the object.
const doneAtAnnotation = "courier.misospace.dev/done-at"

// defaultDoneRetention is how long a resolved Done run is kept for inspection
// before deletion. The checkpoint lives in the CR's own status and dies with
// it, so deletion is what leaves zero standing footprint.
const defaultDoneRetention = 5 * time.Minute

// reapDone applies the terminal-run cleanup policy to a Done run whose source
// work has already resolved: retain briefly, then delete. NeedsHuman and
// Failed runs never reach this path and are never reaped.
func (r *CoderRunReconciler) reapDone(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	now := r.clock()
	retention := r.doneRetention()
	doneAt, ok := doneAtMarker(run)
	if !ok {
		// First Done observation, or an unreadable marker: start (or restart)
		// the retention window and never delete on the same pass.
		before := run.DeepCopy()
		if run.Annotations == nil {
			run.Annotations = map[string]string{}
		}
		run.Annotations[doneAtAnnotation] = now.UTC().Format(time.RFC3339)
		if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: retention}, nil
	}
	remaining := doneAt.Add(retention).Sub(now)
	if remaining <= 0 {
		// NotFound after an attempted delete is success; a transient failure
		// returns and retries. Owned coordinator pods are garbage-collected
		// through their owner references.
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, run))
	}
	// A future-dated marker (clock skew) must not delete early or yield a
	// negative wait. Capping the wait at the retention window keeps it
	// bounded; each reconcile re-derives the true remaining time from the
	// durable marker, so a skew only costs extra wakes, never an early delete.
	if remaining > retention {
		remaining = retention
	}
	return ctrl.Result{RequeueAfter: remaining}, nil
}

// doneAtMarker parses the durable Done marker. A missing or malformed value
// reports false, which the caller treats as "begin a fresh window", never as
// "eligible for immediate deletion".
func doneAtMarker(run *courierv1alpha1.CoderRun) (time.Time, bool) {
	raw, ok := run.Annotations[doneAtAnnotation]
	if !ok {
		return time.Time{}, false
	}
	doneAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return doneAt, true
}

// clock returns the reconciler's time source. Nil falls back to time.Now.
func (r *CoderRunReconciler) clock() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// doneRetention returns the configured retention. Zero falls back to the
// package default; there is deliberately no deployment knob for it.
func (r *CoderRunReconciler) doneRetention() time.Duration {
	if r.DoneRetention > 0 {
		return r.DoneRetention
	}
	return defaultDoneRetention
}
