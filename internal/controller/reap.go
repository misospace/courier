package controller

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// Retention annotations record, as an RFC3339 timestamp, when Courier first
// observed a run become reap-eligible in that terminal phase. They are the
// durable retention clock: the creation timestamp is useless here because a run
// can exist far longer than the retention window before it terminalizes, and
// process memory dies with the controller, so the marker must live on the
// object. Each phase gets its own marker because a run can transition between
// terminal phases (a late AwaitingReview -> NeedsHuman close-unmerged): the
// window for the new outcome must start at the transition, not inherit the
// original execution's age.
const (
	doneAtAnnotation       = "courier.misospace.dev/done-at"
	failedAtAnnotation     = "courier.misospace.dev/failed-at"
	needsHumanAtAnnotation = "courier.misospace.dev/needs-human-at"
)

// Retention defaults. Done runs are a short inspection window because their
// source work already resolved; Failed and NeedsHuman runs hold genuine
// outcomes, so they are kept for a week by default before the reaper deletes
// them. The audit floor keeps the N most recent reaped outcome runs per
// repository even after their window expires, so a burst of work never erases
// the recent audit trail.
const (
	defaultDoneRetention       = 5 * time.Minute
	defaultFailedRetention     = 168 * time.Hour
	defaultNeedsHumanRetention = 168 * time.Hour
	defaultTerminalAuditKeep   = 5
)

// reapDone applies the terminal-run cleanup policy to a Done run whose source
// work has already resolved: retain briefly, then delete. The per-repository
// audit floor does not apply to Done runs: their source work is already
// resolved, so the short window is the whole retention, matching the settled
// Done policy.
func (r *CoderRunReconciler) reapDone(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	return r.reapTerminal(ctx, run, doneAtAnnotation, r.doneRetention(), false)
}

// reapTerminal is the generalized terminal reaper shared by Done, Failed, and
// NeedsHuman runs. It retains the run for retention measured from the durable
// per-phase marker, then deletes it once the audit floor no longer covers it.
// A missing or malformed marker starts a fresh window and never deletes on the
// same pass; a future-dated marker (clock skew) is capped at the window and
// never deletes early. When applyAuditFloor is set, the most recent runs in the
// floor-governed phases are retained per repository even after their window
// expires. The caller must have already established the run's lifecycle report
// reached a positive, settled state — reapTerminal refuses to delete a run whose
// only durable retry intent is still pending.
func (r *CoderRunReconciler) reapTerminal(ctx context.Context, run *courierv1alpha1.CoderRun, marker string, retention time.Duration, applyAuditFloor bool) (ctrl.Result, error) {
	now := r.clock()
	at, ok := reapMarker(run, marker)
	if !ok {
		// First eligible observation, or an unreadable marker: start (or
		// restart) the retention window and never delete on the same pass.
		before := run.DeepCopy()
		if run.Annotations == nil {
			run.Annotations = map[string]string{}
		}
		run.Annotations[marker] = now.UTC().Format(time.RFC3339)
		if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: retention}, nil
	}
	remaining := at.Add(retention).Sub(now)
	if remaining > 0 {
		// A future-dated marker (clock skew) must not delete early or yield a
		// negative wait. Capping the wait at the retention window keeps it
		// bounded; each reconcile re-derives the true remaining time from the
		// durable marker, so a skew only costs extra wakes, never an early
		// delete.
		if remaining > retention {
			remaining = retention
		}
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	// Past the window. The audit floor keeps the most recent runs per
	// repository even after their window expires, so a burst of work never
	// erases the recent audit trail.
	if applyAuditFloor {
		keep, err := r.withinAuditFloor(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if keep {
			// Re-evaluate at the retention cadence: a later, more recent
			// floor-governed run may push this one out of the audit floor.
			return ctrl.Result{RequeueAfter: retention}, nil
		}
	}
	// NotFound after an attempted delete is success; a transient failure
	// returns and retries. Owned coordinator pods and evidence Secrets are
	// garbage-collected through their owner references. The counter records the
	// accepted delete request, not a confirmed removal: a finalizer (the secure
	// topology's) can hold the object after the API server accepts the delete,
	// and courier_coderuns_oldest_terminal_age_seconds is what shows such a run
	// is still retained.
	// Delete-boundary fence (#606): between this reconcile's initial read and
	// this delete request, an operator can add the retry annotation, which the
	// snapshot cannot see — deleting then drops durable intent with no
	// predecessor object on which syncRetry could persist the Requested record.
	// Re-read the live object and re-verify the gate immediately before issuing
	// the delete, and carry UID/resourceVersion preconditions so any write that
	// lands after the re-read — the annotation above all — defeats the delete
	// with a conflict the workqueue retries, re-entering the dispatch on a
	// snapshot that sees the intent. A NotFound on the re-read is the same
	// already-deleted success the delete itself accepts.
	live := &courierv1alpha1.CoderRun{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(run), live); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if hold, why := liveRetryHold(run, live); hold {
		log.FromContext(ctx).V(1).Info("terminal run gained durable retry intent at the delete boundary; leaving it for the next reconcile",
			"run", run.Name, "phase", run.Status.Phase, "why", why)
		return ctrl.Result{}, nil
	}
	if err := r.Delete(ctx, live, client.Preconditions{
		UID:             &live.UID,
		ResourceVersion: &live.ResourceVersion,
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	r.metrics().ObserveReapRequest(run.Status.Phase)
	return ctrl.Result{}, nil
}

// withinAuditFloor reports whether run is still covered by the per-repository
// audit floor: the most recent floor-governed runs are retained regardless of
// age. The candidate is covered while fewer than the floor's count of
// same-repository floor-governed runs are newer than it. A non-positive floor
// disables the guard.
func (r *CoderRunReconciler) withinAuditFloor(ctx context.Context, run *courierv1alpha1.CoderRun) (bool, error) {
	keep := r.terminalAuditKeep()
	if keep <= 0 {
		return false, nil
	}
	var runs courierv1alpha1.CoderRunList
	if err := r.List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
		return false, err
	}
	newer := 0
	for i := range runs.Items {
		other := &runs.Items[i]
		if other.Name == run.Name || other.Spec.Repo != run.Spec.Repo {
			continue
		}
		if !auditFloorPhase(other.Status.Phase) {
			continue
		}
		if terminalNewer(other, run) {
			newer++
		}
	}
	return newer < keep, nil
}

// auditFloorPhase reports whether a run in phase is governed by — and so
// consumes a slot of — the per-repository audit floor. The floor exists to keep
// the recent outcome trail of runs the reaper would otherwise delete, so
// exactly the reaped long-term outcomes participate: Failed and NeedsHuman.
// Done runs are exempt from the floor (their source work is already resolved)
// and AwaitingReview, HandedOff, and BlockedExternal are never reaped at all.
// Counting an exempt or unreaped run as a newer entry would let a burst of
// short-lived Done runs, or one permanently held AwaitingReview run, drain the
// protected quota and permanently delete the runs the floor promises to keep.
func auditFloorPhase(phase courierv1alpha1.Phase) bool {
	switch phase {
	case courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseNeedsHuman:
		return true
	default:
		return false
	}
}

// terminalNewer orders two terminal runs for the audit floor: by terminal time,
// and — because timestamps serialize at second precision and batch-created runs
// can tie — by name as a deterministic tiebreak. The total order means exactly
// the floor's count of runs is retained, never an ambiguous tie group.
func terminalNewer(a, b *courierv1alpha1.CoderRun) bool {
	at, bt := terminalTime(a), terminalTime(b)
	if !at.Equal(bt) {
		return at.After(bt)
	}
	return a.Name > b.Name
}

// terminalTime is the instant a terminal run became terminal for audit-floor
// ordering: the set-once FinishedAt, falling back to the object's creation
// timestamp for legacy runs recorded before FinishedAt existed.
func terminalTime(run *courierv1alpha1.CoderRun) time.Time {
	if run.Status.FinishedAt != nil {
		return run.Status.FinishedAt.Time
	}
	return run.CreationTimestamp.Time
}

// terminalPhase reports whether phase is a terminal lifecycle phase. The
// oldest-terminal-age gauge reports every terminal phase, including the ones
// the reaper never deletes, so an operator sees a held AwaitingReview run aging
// too; the audit floor narrows to auditFloorPhase instead.
func terminalPhase(phase courierv1alpha1.Phase) bool {
	switch phase {
	case courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseHandedOff,
		courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseDone,
		courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseBlockedExternal:
		return true
	default:
		return false
	}
}

// retryHoldsReap reports whether a run carries an outstanding (Requested)
// explicit-retry record (#97/#606) that makes it non-reap-eligible. Deleting a
// run whose retry request may already have committed source-side would drop
// durable intent with the object, so — exactly like a run that still owes a
// source park close — deletion is held while the intent is outstanding.
// syncRetry drives the record and holds the run while it is Requested; this
// predicate is the reap-decision gate the terminal dispatch checks, so the
// invariant holds even if a reconcile reaches the reap decision with a record
// still in Requested. A settled record does not hold, and the retention
// window is never restarted or extended: a retry never extends a predecessor's
// retention beyond its ordinary window.
func retryHoldsReap(run *courierv1alpha1.CoderRun) bool {
	return run != nil && run.Status.RetryRequest != nil &&
		run.Status.RetryRequest.State == courierv1alpha1.RetryRequestStateRequested
}

// liveRetryHold reports whether the live object, re-read at the delete
// boundary, carries durable retry intent this reconcile's snapshot could not
// see. A Requested record holds on any phase: it is durable intent by
// definition. A new annotation value holds on every reaped phase except Done —
// every phase the terminal dispatch drives through syncRetry, so the next
// reconcile translates it (or refuses it for a non-retryable phase) — because
// Done's resolve path never reaches syncRetry and an annotation there cannot
// be honored, so holding the reaper on it would wedge cleanup forever. The
// annotation must be new to this reconcile: an annotation the snapshot already
// carried was seen by syncRetry at the head of this very dispatch, so a value
// that settled into the record must not hold the reaper forever — a retry
// never extends a predecessor's retention beyond its ordinary window.
func liveRetryHold(snapshot, live *courierv1alpha1.CoderRun) (bool, string) {
	if retryHoldsReap(live) {
		return true, "retry request record is Requested"
	}
	liveID := live.Annotations[courierv1alpha1.RetryAnnotation]
	if snapshot.Status.Phase != courierv1alpha1.PhaseDone &&
		validRetryRequestID(liveID) &&
		liveID != snapshot.Annotations[courierv1alpha1.RetryAnnotation] {
		return true, "retry annotation is new to this reconcile"
	}
	return false, ""
}

// reapMarker parses a durable per-phase retention marker. A missing or
// malformed value reports false, which the caller treats as "begin a fresh
// window", never as "eligible for immediate deletion".
func reapMarker(run *courierv1alpha1.CoderRun, marker string) (time.Time, bool) {
	raw, ok := run.Annotations[marker]
	if !ok {
		return time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return at, true
}

// doneAtMarker parses the durable Done marker, preserving the original helper
// name used by the Done resolve path.
func doneAtMarker(run *courierv1alpha1.CoderRun) (time.Time, bool) {
	return reapMarker(run, doneAtAnnotation)
}

// clock returns the reconciler's time source. Nil falls back to time.Now.
func (r *CoderRunReconciler) clock() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// doneRetention returns the configured Done retention. Zero falls back to the
// package default.
func (r *CoderRunReconciler) doneRetention() time.Duration {
	if r.DoneRetention > 0 {
		return r.DoneRetention
	}
	return defaultDoneRetention
}

// failedRetention returns the configured Failed retention. Zero falls back to
// the package default.
func (r *CoderRunReconciler) failedRetention() time.Duration {
	if r.FailedRetention > 0 {
		return r.FailedRetention
	}
	return defaultFailedRetention
}

// needsHumanRetention returns the configured NeedsHuman retention. Zero falls
// back to the package default.
func (r *CoderRunReconciler) needsHumanRetention() time.Duration {
	if r.NeedsHumanRetention > 0 {
		return r.NeedsHumanRetention
	}
	return defaultNeedsHumanRetention
}

// provisioningCollisionGrace returns the configured stuck-terminating
// collision grace. Zero falls back to the package default.
func (r *CoderRunReconciler) provisioningCollisionGrace() time.Duration {
	if r.ProvisioningCollisionGrace > 0 {
		return r.ProvisioningCollisionGrace
	}
	return defaultProvisioningCollisionGrace
}

// terminalAuditKeep returns the configured per-repository audit floor. Zero
// falls back to the package default; a negative value disables the floor.
func (r *CoderRunReconciler) terminalAuditKeep() int {
	if r.TerminalAuditKeep != 0 {
		return r.TerminalAuditKeep
	}
	return defaultTerminalAuditKeep
}
