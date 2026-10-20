package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
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

const conditionCapacityCooldownRecorded = "CapacityCooldownRecorded"

func capacityCooldownRecorded(run *courierv1alpha1.CoderRun) bool {
	condition := apimeta.FindStatusCondition(run.Status.Conditions, conditionCapacityCooldownRecorded)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func (r *CoderRunReconciler) capacityCooldownWindow() time.Duration {
	if r.CapacityCooldownWindow > 0 {
		return r.CapacityCooldownWindow
	}
	return defaultCapacityCooldownWindow
}

// isCapacityExhaustionRun reports a run that ended on a recoverable capacity
// handoff: the BlockedExternal phase carrying one of the durable capacity
// categories — the confirmed capacity exhaustion, the model-provider
// availability handoff (provider-unavailable), the forge-read availability
// handoff (forge-unavailable), or the spent model/agent child-process budget
// (model-transport-error). All of them mean "this capacity could not complete
// the attempt for availability reasons"; none is a task-correctness verdict.
// Phase and category are both required — a plain external block is not a
// capacity event, and a capacity category on any other phase proves nothing
// this path may act on.
func isCapacityExhaustionRun(run *courierv1alpha1.CoderRun) bool {
	if run == nil || run.Status.Phase != courierv1alpha1.PhaseBlockedExternal || run.Status.TerminalDiagnostic == nil {
		return false
	}
	switch run.Status.TerminalDiagnostic.Category {
	case courierv1alpha1.TerminalDiagnosticCategoryCapacityExhaustion,
		courierv1alpha1.TerminalDiagnosticCategoryProviderUnavailable,
		courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable,
		courierv1alpha1.TerminalDiagnosticCategoryModelTransportError:
		return true
	}
	return false
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
	// Cooldown registration depends only on the trusted persisted diagnostic,
	// never on source report availability.
	if err := r.recordCapacityCooldown(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	// Ownership handoff remains gated on the positively accepted report.
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

	if !safeCapacityWorkspace(run.Status.WorkspaceSafety, run.Status.Branch, run.Status.HeadSHA) {
		return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryUnsafeWork,
			"trusted workspace evidence does not prove partial work is safely held remotely; ownership remains held")
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
	lane := &courierv1alpha1.LaneProfile{}
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, lane); err == nil {
		handoff.CapacityIdentity = lane.CapacityIdentity()
	} else {
		return ctrl.Result{}, r.setCapacityRecovery(ctx, run, metav1.ConditionFalse, capacityRecoveryRetryable, "read capacity identity for source settlement: "+err.Error())
	}
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

func safeCapacityWorkspace(evidence *courierv1alpha1.WorkspaceSafetyEvidence, branch, head string) bool {
	if evidence == nil {
		return false
	}
	switch evidence.State {
	case "empty":
		return true
	case "pushed":
		return evidence.RemoteHead != "" && evidence.Head != "" && evidence.RemoteHead == evidence.Head &&
			evidence.Branch != "" && evidence.Branch == branch && branch != ""
	default:
		return false
	}
}

// recordCapacityCooldown stamps (or monotonically extends) the capacity
// identity's cooldown on its LaneProfile status. The write is idempotent per
// run: a retried recovery for the same run never double-counts, and a newer
// exhaustion from a different run extends the window so repeatedly failing
// capacity keeps pausing. Unrelated capacity identities are untouched.
func (r *CoderRunReconciler) recordCapacityCooldown(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	// Persist an attempt-owned event marker before changing shared lane state.
	// This makes retries and A/B/A interleavings idempotent without a growing
	// history list on the LaneProfile.
	if !capacityCooldownRecorded(run) {
		before := run.DeepCopy()
		apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: conditionCapacityCooldownRecorded, Status: metav1.ConditionTrue,
			Reason: "Recorded", Message: "confirmed capacity exhaustion registered",
			LastTransitionTime: metav1.NewTime(r.clock()),
		})
		if err := r.patchStatus(ctx, before, run); err != nil {
			return err
		}
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.recordCapacityCooldownLane(ctx, run)
	})
}

func (r *CoderRunReconciler) recordCapacityCooldownLane(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	lane := &courierv1alpha1.LaneProfile{}
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, lane); err != nil {
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
	// Recompute the active window from durable per-run first-event timestamps.
	// This makes cross-object patch gaps harmless and bounds the evidence by
	// the cooldown window rather than accumulating a lane-owned history list.
	var runs courierv1alpha1.CoderRunList
	if err := r.reader().List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
		return fmt.Errorf("list capacity exhaustion evidence: %w", err)
	}
	var lanes courierv1alpha1.LaneProfileList
	if err := r.reader().List(ctx, &lanes, client.InNamespace(run.Namespace)); err != nil {
		return fmt.Errorf("list capacity evidence lanes: %w", err)
	}
	laneByName := make(map[string]*courierv1alpha1.LaneProfile, len(lanes.Items))
	for i := range lanes.Items {
		laneByName[lanes.Items[i].Name] = &lanes.Items[i]
	}
	// The run may have been patched through the direct client while this list
	// came from a lagging cache. Overlay it so its event is never undercounted.
	foundCurrent := false
	for i := range runs.Items {
		if runs.Items[i].UID == run.UID && runs.Items[i].Name == run.Name {
			runs.Items[i] = *run.DeepCopy()
			foundCurrent = true
			break
		}
	}
	if !foundCurrent {
		runs.Items = append(runs.Items, *run.DeepCopy())
	}
	windowStart := r.clock().UTC().Add(-r.capacityCooldownWindow())
	count := 0
	var latest time.Time
	latestRun := ""
	for i := range runs.Items {
		event := &runs.Items[i]
		if !isCapacityExhaustionRun(event) || event.Spec.Lane == "" {
			continue
		}
		eventLane := laneByName[event.Spec.Lane]
		if eventLane == nil || eventLane.CapacityIdentity() != identity {
			continue
		}
		marker := apimeta.FindStatusCondition(event.Status.Conditions, conditionCapacityCooldownRecorded)
		if marker == nil || marker.Status != metav1.ConditionTrue || marker.LastTransitionTime.Time.Before(windowStart) {
			continue
		}
		count++
		if marker.LastTransitionTime.Time.After(latest) || (marker.LastTransitionTime.Time.Equal(latest) && event.Name > latestRun) {
			latest = marker.LastTransitionTime.Time
			latestRun = event.Name
		}
	}
	before := lane.DeepCopy()
	if count == 0 {
		if lane.Status.CapacityCooldown != nil && lane.Status.CapacityCooldown.CapacityID == identity {
			lane.Status.CapacityCooldown = nil
		}
	} else {
		lane.Status.CapacityCooldown = &courierv1alpha1.CapacityCooldown{
			CapacityID:       identity,
			Reason:           fmt.Sprintf("run %s reported a recoverable capacity handoff", latestRun),
			LastExhaustedRun: latestRun,
			ExhaustionCount:  count,
			NextEligibleAt:   metav1.NewTime(latest.Add(r.capacityCooldownWindow())),
		}
	}
	if equality.Semantic.DeepEqual(before.Status.CapacityCooldown, lane.Status.CapacityCooldown) {
		return nil
	}
	if err := r.Status().Patch(ctx, lane, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("record capacity cooldown on lane %s: %w", lane.Name, err)
	}
	if lane.Status.CapacityCooldown == nil {
		log.FromContext(ctx).Info("capacity cooldown cleared: no live exhaustion markers", "lane", lane.Name, "capacityID", identity)
		return nil
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
