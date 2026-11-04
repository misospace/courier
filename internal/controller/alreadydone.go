package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// sourceCloseCondition tracks the operator's evidence-gated close of the run's
// source work item (an already-done conclusion). Status True with the Closed
// reason means the source applied the close and the run remains AlreadyDone,
// awaiting retention. A False status with one of the source-close refusal
// reasons means the source did not close the work item, and the run is parked
// for human attention. The condition is only ever written for an AlreadyDone
// run's own close; it carries no meaning for any other phase.
const (
	sourceCloseCondition = "SourceClose"

	sourceCloseClosedReason         = "Closed"
	sourceCloseRefusedReason        = "Refused"
	sourceCloseFailedReason         = "Failed"
	sourceCloseSkippedReason        = "Skipped"
	sourceCloseUnsupportedReason    = "Unsupported"
	sourceCloseOwnershipMovedReason = "OwnershipMoved"

	// sourceCloseCommentedCondition marks that the run's settled close
	// outcome was announced on the source work item. It is set only after a
	// successful post, so a delivery failure retries while the settlement
	// still re-runs; a crash between the post and the write may repeat one
	// comment, which an operator can recognize, rather than silently drop
	// the announcement.
	sourceCloseCommentedCondition = "SourceCloseCommented"
)

// alreadyDoneCommentBound bounds the operator-authored source-issue comment.
// A longer body is hard-truncated on a rune boundary so a verbose proposal can
// never overflow the forge's comment limit or drag the outcome along.
const alreadyDoneCommentBound = 1500

// WorkItemCommenter posts a bounded, operator-authored comment to the run's
// source work item through the operator's own forge write identity. It is the
// delivery seam for the already-done close notification; the body is already
// bounded by the caller. It is strictly best-effort: a nil implementation or a
// failure leaves the run's outcome unchanged, because the comment is a
// notification, never a settlement authority.
type WorkItemCommenter interface {
	CommentWorkItem(ctx context.Context, run *courierv1alpha1.CoderRun, body string) error
}

// resolveIssueNoChangePhase classifies a trusted no_change ending for a
// resolve-issue run. A durable, incarnation-fenced grounded close proposal
// (status.noChange) that fences to this pod's UID upgrades the conclusion to
// AlreadyDone: the ask is claimed satisfied by existing work, with the source
// as the only authority on whether that closes the item. A no-change ending
// without such a proposal is evidence-incomplete and parks NeedsHuman — it
// never resolves on the coordinator's word alone and never routes to
// AwaitingReview, which would imply an unverified pull request awaiting review.
func resolveIssueNoChangePhase(run *courierv1alpha1.CoderRun, podUID string) (courierv1alpha1.Phase, bool) {
	if run != nil {
		if nc := run.Status.NoChange; nc != nil && nc.CoordinatorPodUID == podUID {
			return courierv1alpha1.PhaseAlreadyDone, true
		}
	}
	return courierv1alpha1.PhaseNeedsHuman, false
}

// alreadyDoneLifecycle derives the source close report for an already-done
// conclusion. The close proposal is re-derived from the durable,
// incarnation-fenced status.noChange record, so it is identical across
// reconciles, restarts, and retry replays. The state is empty: an already-done
// conclusion performs no work-item state transition — the close itself, through
// the source's own evidence-gated close policy, is the settlement.
func (r *CoderRunReconciler) alreadyDoneLifecycle(run *courierv1alpha1.CoderRun, intent terminalLifecycleIntent) source.Lifecycle {
	lifecycle := source.Lifecycle{
		Result:         source.ResultAlreadyDone,
		Error:          intent.error,
		IdempotencyKey: lifecycleIdempotencyKey(run, courierv1alpha1.PhaseAlreadyDone),
	}
	if nc := run.Status.NoChange; nc != nil {
		lifecycle.Close = &source.CloseProposal{
			Criteria: toSourceCloseCriteria(nc.Criteria),
			Evidence: nc.Evidence,
		}
	}
	if run.Status.Telemetry != nil {
		if raw, err := json.Marshal(run.Status.Telemetry); err == nil {
			lifecycle.Telemetry = raw
		}
	}
	return lifecycle
}

func toSourceCloseCriteria(criteria []courierv1alpha1.NoChangeCloseCriterion) []source.CloseCriterion {
	out := make([]source.CloseCriterion, 0, len(criteria))
	for _, c := range criteria {
		out = append(out, source.CloseCriterion{Criterion: c.Criterion, Path: c.Path, Excerpt: c.Excerpt})
	}
	return out
}

// settleAlreadyDoneClose settles the run's evidence-gated already-done
// conclusion through the source's own close policy and records the outcome. It
// is the sole consumer of the run's close proposal, invoked on the first
// terminal transition and again on every re-settlement of a close that has not
// reached a positive outcome, so a restart re-derives and re-presents the same
// proposal under the same idempotency key.
//
// The source's resolution is the only authority. A closed work item settles the
// lifecycle as published and leaves the run in AlreadyDone, awaiting retention.
// Every non-closed resolution — a policy refusal, a close-execution failure, an
// unconfirmable replay, a source that offers no close path, or a permanent
// refusal that moved ownership of the work — parks the run for human attention.
// A superseded rejection drops the report without a retry, and a transient
// failure leaves a pending report for the next re-settlement. A best-effort
// source-issue comment accompanies each settled outcome.
func (r *CoderRunReconciler) settleAlreadyDoneClose(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, intent terminalLifecycleIntent) (ctrl.Result, error) {
	if run.Status.NoChange == nil {
		// A run without a durable proposal cannot be evidence-gated: fail closed
		// to human attention rather than reaping it as a success.
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseUnsupportedReason, "no grounded close proposal is recorded on the run")
	}
	reporter, ok := adapter.(source.CloseReporter)
	if !ok {
		// The source offers no evidence-gated close path. It must refuse the
		// result, and the operator must not imitate settlement with an ordinary
		// lifecycle report, which the source contract forbids.
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseUnsupportedReason, "source does not offer an evidence-gated close path")
	}
	lifecycle := r.alreadyDoneLifecycle(run, intent)
	resolution, err := reporter.ReportClose(ctx, item, lifecycle)
	switch {
	case err == nil && resolution.Action == source.CloseActionClosed:
		r.setSourceClose(run, true, sourceCloseClosedReason, resolution)
		r.commentAlreadyDone(ctx, run, true, sourceCloseClosedReason, resolution)
		return ctrl.Result{}, r.markLifecycleReportOutcome(ctx, run, true, lifecycleReportPublishedReason, "", "", lifecycleReportAcceptedSupportReason)
	case err == nil:
		reason := sourceCloseReasonForAction(resolution.Action)
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, reason, closeResolutionDetail(resolution))
	case errors.Is(err, source.ErrSuperseded):
		log.FromContext(ctx).Info("dropping superseded already-done close report",
			"run", run.Name, "error", err.Error())
		return ctrl.Result{}, r.markLifecycleReportOutcome(ctx, run, true, lifecycleReportSupersededReason, err.Error(), "", lifecycleReportSupersededReason)
	case errors.Is(err, source.ErrUnsupportedOutcome):
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseUnsupportedReason, err.Error())
	case errors.Is(err, source.ErrPermanentRefusal):
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseOwnershipMovedReason, err.Error())
	default:
		log.FromContext(ctx).Info("already-done close report failed; will retry",
			"run", run.Name, "error", err.Error())
		return ctrl.Result{RequeueAfter: lifecycleReportRequeueDelay}, r.markAlreadyDoneClosePending(ctx, run, intent)
	}
}

// markAlreadyDoneClosePending leaves an unsettled already-done close as a
// pending lifecycle report for the next re-settlement, preserving the pending
// retry intent the way the ordinary terminal path does. The run is never held
// by the report: it stays in its terminal phase and is re-presented on the
// bounded retry cadence.
func (r *CoderRunReconciler) markAlreadyDoneClosePending(ctx context.Context, run *courierv1alpha1.CoderRun, intent terminalLifecycleIntent) error {
	retryToken := run.Status.LifecycleReportActiveRetryToken
	if retryToken == "" && lifecycleReportUnsupported(run) && lifecycleReportRetryRequested(run) {
		retryToken = run.Annotations[retryLifecycleReportAnnotation]
	}
	return r.markLifecycleReportOutcome(ctx, run, false, pendingLifecycleReason(intent), intent.error, retryToken, lifecycleReportRetryableReason)
}

// parkAlreadyDoneClose records a non-closing close outcome on the run and
// transitions it from AlreadyDone to NeedsHuman. The SourceClose condition is
// written in the same status patch as the phase change, so the recorded reason
// for the parking is never split from it. A best-effort source-issue comment
// precedes the transition so the human reviewer has the source's answer beside
// the parked outcome.
func (r *CoderRunReconciler) parkAlreadyDoneClose(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, reason, detail string) (ctrl.Result, error) {
	resolution := closeResolutionFromDetail(detail)
	r.setSourceClose(run, false, reason, resolution)
	r.commentAlreadyDone(ctx, run, false, reason, resolution)
	park := terminalLifecycleIntent{
		error: alreadyDoneCloseParkMessage(reason, detail),
	}
	log.FromContext(ctx).Info("already-done close not applied; parking for human attention",
		"run", run.Name, "reason", reason, "detail", detail)
	return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "", park, nil)
}

// setSourceClose records the close outcome on the run's SourceClose condition.
// It is an in-memory mutation: the caller persists it through the next status
// patch, so the reason lands in the same write as any accompanying phase change.
func (r *CoderRunReconciler) setSourceClose(run *courierv1alpha1.CoderRun, closed bool, reason string, resolution source.CloseResolution) {
	statusValue := metav1.ConditionTrue
	if !closed {
		statusValue = metav1.ConditionFalse
	}
	message := closeResolutionDetail(resolution)
	if message == "" {
		if closed {
			message = "work item closed"
		} else {
			message = reason
		}
	}
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               sourceCloseCondition,
		Status:             statusValue,
		Reason:             reason,
		Message:            boundedAlreadyDoneText(message),
		LastTransitionTime: metav1.NewTime(r.clock()),
	})
}

// commentAlreadyDone posts the bounded operator-authored notification about the
// run's close outcome to its source work item. It is best-effort: a missing
// commenter or a delivery failure is logged and never alters the run's
// outcome. A run whose settled outcome was already announced is never
// announced twice.
func (r *CoderRunReconciler) commentAlreadyDone(ctx context.Context, run *courierv1alpha1.CoderRun, closed bool, reason string, resolution source.CloseResolution) {
	if r.WorkItemComments == nil {
		return
	}
	if alreadyDoneCommented(run) {
		return
	}
	body := alreadyDoneCommentBody(run, closed, reason, resolution)
	if body == "" {
		return
	}
	if err := r.WorkItemComments.CommentWorkItem(ctx, run, body); err != nil {
		log.FromContext(ctx).V(1).Info("source-issue comment for already-done run failed",
			"run", run.Name, "error", err.Error())
		return
	}
	r.markAlreadyDoneCommented(run)
}

// commentAlreadyDoneHold announces an evidence-incomplete no-change hold on
// the source work item: the run concluded no change without a grounded close
// proposal, so no PR is coming and no closure was attempted. Best-effort and
// once per run, exactly like the settled-outcome notification.
func (r *CoderRunReconciler) commentAlreadyDoneHold(ctx context.Context, run *courierv1alpha1.CoderRun) {
	if r.WorkItemComments == nil {
		return
	}
	if alreadyDoneCommented(run) {
		return
	}
	body := "Courier will not open a pull request for this issue and did not attempt to close it: the run concluded the request was already satisfied but recorded no grounded close evidence, so the conclusion is held for a human. No publication is claimed."
	before := run.DeepCopy()
	if err := r.WorkItemComments.CommentWorkItem(ctx, run, body); err != nil {
		log.FromContext(ctx).V(1).Info("source-issue hold comment failed",
			"run", run.Name, "error", err.Error())
		return
	}
	r.markAlreadyDoneCommented(run)
	if err := r.patchStatus(ctx, before, run); err != nil {
		log.FromContext(ctx).V(1).Info("recording the source-issue hold comment failed",
			"run", run.Name, "error", err.Error())
	}
}

// alreadyDoneCommented reports whether this run's outcome was already
// announced on its source work item.
func alreadyDoneCommented(run *courierv1alpha1.CoderRun) bool {
	cond := apimeta.FindStatusCondition(run.Status.Conditions, sourceCloseCommentedCondition)
	return cond != nil && cond.Status == metav1.ConditionTrue
}

// markAlreadyDoneCommented records the announce-once marker in memory; the
// caller persists it with the next status patch.
func (r *CoderRunReconciler) markAlreadyDoneCommented(run *courierv1alpha1.CoderRun) {
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               sourceCloseCommentedCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "Posted",
		Message:            "the run's outcome was announced on the source work item",
		LastTransitionTime: metav1.NewTime(r.clock()),
	})
}

// alreadyDoneCommentBody renders the bounded operator notification about the
// run's close outcome. It carries the coordinator's assessment and its grounded
// criteria so a human reviewer can verify the conclusion against the
// repository, and it names the source's answer when the close was not applied.
func alreadyDoneCommentBody(run *courierv1alpha1.CoderRun, closed bool, reason string, resolution source.CloseResolution) string {
	var b strings.Builder
	detail := closeResolutionDetail(resolution)
	if closed {
		b.WriteString("Courier closed this work item: its request is already satisfied by existing work.")
	} else {
		b.WriteString("Courier could not close this work item automatically (" + strings.ToLower(reason) + ")")
		if detail != "" {
			b.WriteString(": " + detail)
		}
		b.WriteString("; a human has been asked to review.")
	}
	if nc := run.Status.NoChange; nc != nil {
		if nc.Evidence != "" {
			b.WriteString(" Coordinator assessment: " + nc.Evidence + ".")
		}
		if len(nc.Criteria) > 0 {
			b.WriteString(" Grounded criteria:")
			for i, c := range nc.Criteria {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(fmt.Sprintf("%s (in %s)", c.Criterion, c.Path))
			}
		}
	}
	return boundAlreadyDoneComment(b.String())
}

// boundAlreadyDoneComment trims and hard-truncates a notification body on a
// rune boundary so a verbose proposal can never overflow the delivery limit.
func boundAlreadyDoneComment(s string) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= alreadyDoneCommentBound {
		return s
	}
	return string(r[:alreadyDoneCommentBound])
}

// boundedAlreadyDoneText trims and hard-truncates a SourceClose condition
// message on a rune boundary.
func boundedAlreadyDoneText(s string) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= alreadyDoneCommentBound {
		return s
	}
	return string(r[:alreadyDoneCommentBound])
}

// sourceCloseReasonForAction maps a non-closing close action to its
// SourceClose reason. An unrecognized action fails closed to the generic
// failure reason rather than being silently treated as a close.
func sourceCloseReasonForAction(action string) string {
	switch action {
	case source.CloseActionRefused:
		return sourceCloseRefusedReason
	case source.CloseActionFailed:
		return sourceCloseFailedReason
	case source.CloseActionSkipped:
		return sourceCloseSkippedReason
	default:
		return sourceCloseFailedReason
	}
}

// closeResolutionDetail renders the source's single-line explanation of a close
// outcome, preferring its specific policy reasons over the generic reason and
// falling back to the action itself.
func closeResolutionDetail(resolution source.CloseResolution) string {
	if len(resolution.Reasons) > 0 {
		return strings.Join(resolution.Reasons, "; ")
	}
	if strings.TrimSpace(resolution.Reason) != "" {
		return resolution.Reason
	}
	return resolution.Action
}

// closeResolutionFromDetail reconstructs a resolution-shaped value from a
// free-form detail so the SourceClose condition and the comment share one
// bounded message.
func closeResolutionFromDetail(detail string) source.CloseResolution {
	return source.CloseResolution{Reason: detail}
}

// alreadyDoneCloseParkMessage is the source-facing explanation a run parked for
// human attention carries after its evidence-gated close was not applied. It
// names the close outcome and the source's answer so the reviewer and the
// source share the same bounded cause.
func alreadyDoneCloseParkMessage(reason, detail string) string {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		return "evidence-gated close was not applied (" + reason + "); human review required"
	}
	return "evidence-gated close was not applied (" + reason + "): " + detail
}
