package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// defaultCapacityCooldownWindow bounds how long an exhausted capacity
// identity pauses new discovery and admission after a confirmed exhaustion
// (#327). It is a bounded re-claim control, not a provider reset time: the
// real reset belongs to the provider and Courier never invents one, so the
// window only guarantees the exhausted capacity stops hammering while
// unrelated capacity keeps working, and a still-dead pool simply re-enters
// the window on its next confirmed exhaustion.
const defaultCapacityCooldownWindow = 15 * time.Minute

// Capacity-recovery condition reasons written on CoderRun.Status.Conditions
// under ConditionCapacityRecovery. Settled is the only success; Unsupported,
// Stale, and UnsafeWork stop automatic attempts and wait for an operator or
// a source-side mechanism; PendingWriters and Retryable re-drive on a bounded
// timer.
const (
	capacityRecoverySettled        = "Settled"
	capacityRecoveryUnsupported    = "Unsupported"
	capacityRecoveryStale          = "Stale"
	capacityRecoveryUnsafeWork     = "UnsafeWork"
	capacityRecoveryPendingWriters = "PendingWriters"
	capacityRecoveryRetryable      = "Retryable"
)

func (r *CoderRunReconciler) capacityCooldownWindow() time.Duration {
	if r.CapacityCooldownWindow > 0 {
		return r.CapacityCooldownWindow
	}
	return defaultCapacityCooldownWindow
}

// isCapacityExhaustionRun reports a run that ended on the structured
// capacity-exhaustion signal: the recoverable BlockedExternal phase carrying
// a confirmed capacity-exhaustion terminal diagnostic. Neither half is
// sufficient — a plain external block is not a capacity event, and a
// capacity-exhaustion category on any other phase proves nothing this path
// may act on.
func isCapacityExhaustionRun(run *courierv1alpha1.CoderRun) bool {
	return run != nil &&
		run.Status.Phase == courierv1alpha1.PhaseBlockedExternal &&
		run.Status.TerminalDiagnostic != nil &&
		run.Status.TerminalDiagnostic.Category == courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion
}

// capacityRecoveryPending reports an exhaustion run whose fenced settlement
// has not reached a durable automation-stopping state. True means a later
// reconcile (or a restarted controller) should re-drive settlement.
func capacityRecoveryPending(run *courierv1alpha1.CoderRun) bool {
	if !isCapacityExhaustionRun(run) {
		return false
	}
	cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionCapacityRecovery)
	if cond == nil {
		return true
	}
	if cond.Status == metav1.ConditionTrue && cond.Reason == capacityRecoverySettled {
		return false
	}
	switch cond.Reason {
	case capacityRecoveryUnsupported, capacityRecoveryStale, capacityRecoveryUnsafeWork:
		return false
	}
	return true
}

// recoverCapacityExhaustion drives the #327 recovery settlement for one
// exhaustion run: it records the capacity cooldown, refuses to release while
// any writer for the run is still alive or the unpushed-work state is
// unresolved, then settles the attempt through the adapter's optional fenced
// CapacityRecovery boundary. Every outcome is persisted as the run's
// CapacityRecovery condition so a controller restart re-derives exactly this
// decision from the cluster. An adapter without the capability is recorded
// Unsupported — the core never simulates fencing with ordinary Release.
func (r *CoderRunReconciler) recoverCapacityExhaustion(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem) (ctrl.Result, error) {
	// The source must have seen the recoverable lifecycle report before any
	// ownership handoff; until then the report retry path owns the timer.
	report := apimeta.FindStatusCondition(run.Status.Conditions, lifecycleReportedCondition)
	if report == nil || report.Status != metav1.ConditionTrue {
		return ctrl.Result{}, nil
	}
	if report.Reason == "Superseded" {
		// A newer source generation owns the work; this attempt must not
		// settle or release anything, fenced or not.
		return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryStale,
			"the lifecycle report was superseded; a newer source attempt owns the work")
	}

	// Cooldown first, and source-neutral: even an adapter that cannot settle
	// stops its capacity from immediately re-claiming. Idempotent per run.
	if err := r.recordCapacityCooldown(ctx, run); err != nil {
		// No condition is written, so the next reconcile re-drives the whole
		// recovery — the cooldown write must not be silently lost.
		return ctrl.Result{}, err
	}

	// Never allow overlapping writers: settlement waits until every pod of
	// this run's topology has terminated, so a draining worker can never
	// write after ownership moves.
	var pods corev1.PodList
	if err := r.reader().List(ctx, &pods, client.InNamespace(run.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podBelongsToRun(pod, run) {
			continue
		}
		if !pod.DeletionTimestamp.IsZero() || (pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed) {
			return ctrl.Result{RequeueAfter: observationRequeueDelay}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryPendingWriters,
				"run topology still has live pods; settlement waits for every writer to terminate")
		}
	}

	preparer, ok := adapter.(source.CapacityRecoveryPreparer)
	if !ok {
		return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryUnsupported,
			"source does not provide authoritative capacity-recovery proof; ownership remains held")
	}
	key := capacitySettlementKey(run)
	proof, err := preparer.PrepareCapacityRecovery(ctx, item, source.CapacityRecoveryAttempt{RunUID: string(run.UID), IdempotencyKey: key})
	if err != nil {
		if errors.Is(err, source.ErrStaleCapacitySettlement) || errors.Is(err, source.ErrSuperseded) {
			return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryStale, err.Error())
		}
		if errors.Is(err, source.ErrUnsupportedCapacityRecovery) {
			return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryUnsupported, err.Error())
		}
		return ctrl.Result{RequeueAfter: lifecycleReportRequeueDelay}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryRetryable, err.Error())
	}
	if proof.FenceToken == "" || proof.WorkIdentity == "" || proof.SafetyProof == "" {
		return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryUnsafeWork,
			"source returned incomplete authoritative safety proof; ownership remains held")
	}
	handoff := source.CapacityHandoff{FenceToken: proof.FenceToken, IdempotencyKey: key, WorkIdentity: proof.WorkIdentity, Branch: proof.Branch, PR: proof.PR}
	recovery, ok := adapter.(source.CapacityRecovery)
	if !ok {
		return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryUnsupported,
			"source cannot atomically settle capacity recovery; ownership remains held")
	}
	if err := recovery.SettleCapacityExhaustion(ctx, item, handoff); err != nil {
		if errors.Is(err, source.ErrStaleCapacitySettlement) || errors.Is(err, source.ErrSuperseded) {
			return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryStale, err.Error())
		}
		if errors.Is(err, source.ErrUnsupportedCapacityRecovery) {
			return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryUnsupported, err.Error())
		}
		return ctrl.Result{RequeueAfter: lifecycleReportRequeueDelay}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryRetryable, err.Error())
	}
	return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionTrue, capacityRecoverySettled,
		"source accepted authoritative safe handoff and settled this attempt")
}

// recordCapacityCooldown stamps (or monotonically extends) the capacity
// identity's cooldown on its LaneProfile status. The write is idempotent per
// run: a retried recovery for the same run never double-counts, and a newer
// exhaustion from a different run extends the window so repeatedly failing
// capacity keeps pausing. Unrelated capacity identities are untouched.
func (r *CoderRunReconciler) recordCapacityCooldown(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	lane := &courierv1alpha1.LaneProfile{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, lane); err != nil {
		if apierrors.IsNotFound(err) {
			// The lane object is gone; there is no capacity identity left to
			// cool down. Settlement proceeds — the source side is still the
			// authority on ownership.
			log.FromContext(ctx).Info("capacity cooldown skipped: LaneProfile no longer exists",
				"run", run.Name, "lane", run.Spec.Lane)
			return nil
		}
		return fmt.Errorf("get LaneProfile %s/%s: %w", run.Namespace, run.Spec.Lane, err)
	}
	identity := lane.CapacityIdentity()
	reason := fmt.Sprintf("run %s reported confirmed execution-capacity exhaustion", run.Name)
	before := lane.DeepCopy()
	cooldown := lane.Status.CapacityCooldown
	if cooldown == nil || cooldown.CapacityID != identity {
		lane.Status.CapacityCooldown = &courierv1alpha1.CapacityCooldown{
			CapacityID:       identity,
			Reason:           reason,
			LastExhaustedRun: run.Name,
			ExhaustionCount:  1,
			NextEligibleAt:   metav1.NewTime(r.clock().UTC().Add(r.capacityCooldownWindow())),
		}
	} else if cooldown.LastExhaustedRun != run.Name {
		// A retry of this run must not slide the deadline. A distinct
		// exhaustion event starts a fresh window.
		cooldown.ExhaustionCount++
		cooldown.LastExhaustedRun = run.Name
		cooldown.Reason = reason
		cooldown.NextEligibleAt = metav1.NewTime(r.clock().UTC().Add(r.capacityCooldownWindow()))
	}
	if err := r.Status().Patch(ctx, lane, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("record capacity cooldown on lane %s: %w", lane.Name, err)
	}
	log.FromContext(ctx).Info("capacity cooldown recorded",
		"lane", lane.Name, "capacityID", identity,
		"nextEligibleAt", lane.Status.CapacityCooldown.NextEligibleAt.Time,
		"exhaustionCount", lane.Status.CapacityCooldown.ExhaustionCount)
	return nil
}

// setCapacityRecovery persists the recovery condition through the operator
// status patch. It writes the in-memory condition first so the patch diff
// carries it.
func (r *CoderRunReconciler) setCapacityRecovery(ctx context.Context, run *courierv1alpha1.CoderRun, statusValue metav1.ConditionStatus, reason, message string) error {
	before := run.DeepCopy()
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               courierv1alpha1.ConditionCapacityRecovery,
		Status:             statusValue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.clock()),
	})
	if err := r.patchStatus(ctx, before, run); err != nil {
		log.FromContext(ctx).Error(err, "persisting capacity recovery condition failed", "run", run.Name, "reason", reason)
		return err
	}
	return nil
}

// capacitySettlementKey is the deterministic settlement identity of one run
// attempt: retries of the same attempt carry the same key, and no other run
// can present it.
func capacitySettlementKey(run *courierv1alpha1.CoderRun) string {
	return fmt.Sprintf("capacity/%s/%s/%s", run.Namespace, run.Name, string(run.UID))
}
