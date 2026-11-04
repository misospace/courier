package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
)

// alreadyDoneCommentRedactor scrubs credential-shaped text out of every
// non-operator fragment before it is rendered into the source-issue comment.
// It is patterns-only: URL userinfo, bearer/authorization values, credential
// query parameters, and well-known token shapes. Redact is idempotent, so a
// fragment that is already clean passes through unchanged.
var alreadyDoneCommentRedactor = courierlog.NewRedactor()

// sourceCloseCondition tracks the operator's evidence-gated close of the run's
// source work item (an already-done conclusion). Status True with the Closed
// reason means the source applied the close and the run remains AlreadyDone,
// awaiting retention. A False status with one of the source-close refusal
// reasons means the source did not close the work item, and the run is parked
// for human attention. The condition is only ever written for an AlreadyDone
// run's own close; it carries no meaning for any other phase.
const (
	sourceCloseCondition = "SourceClose"

	sourceCloseClosedReason      = "Closed"
	sourceCloseRefusedReason     = "Refused"
	sourceCloseFailedReason      = "Failed"
	sourceCloseSkippedReason     = "Skipped"
	sourceCloseUnsupportedReason = "Unsupported"
	// sourceCloseRefusedPermanentlyReason names a durable source refusal of
	// the close report (a permanent 400/403 classification — which includes
	// the scope gate's moved-ownership denial among other durable refusals).
	// The source's own message, kept in the condition and comment detail,
	// identifies which durable reason it was.
	sourceCloseRefusedPermanentlyReason = "RefusedPermanently"

	// sourceCloseCommentedCondition marks that the run's settled close
	// outcome was announced on the source work item. It is set only after a
	// successful post, so a delivery failure retries while the settlement
	// still re-runs; a crash between the post and the write may repeat one
	// comment, which an operator can recognize, rather than silently drop
	// the announcement.
	sourceCloseCommentedCondition = "SourceCloseCommented"

	// The False status of the announcement condition names why nothing was
	// delivered, so a run never reads as announced when no comment could
	// reach the work item (#727).
	sourceCloseCommentNotConfiguredReason = "CommentIdentityNotConfigured"
	sourceCloseCommentDeliveryReason      = "DeliveryFailed"
)

// alreadyDoneCommentBound bounds the operator-authored source-issue comment.
// The verification links and grounded criteria render ahead of the free-form
// coordinator assessment, so a hard truncation on a rune boundary costs the
// prose, never the structured links; a verbose proposal can still never
// overflow the forge's comment limit or drag the outcome along.
const alreadyDoneCommentBound = 2400

// WorkItemCommenter posts a bounded, operator-authored comment to the run's
// source work item through the operator's own forge write identity. It is the
// delivery seam for the already-done close notification; the body is already
// bounded by the caller. It is strictly best-effort: a nil implementation or a
// failure leaves the run's outcome unchanged, because the comment is a
// notification, never a settlement authority.
type WorkItemCommenter interface {
	CommentWorkItem(ctx context.Context, run *courierv1alpha1.CoderRun, body string) error
}

// WorkItemLinker renders bounded forge-web links for the settled-outcome
// announcement. CommitLink returns the forge's web URL for one commit of a
// repository; FileLink returns the forge's web URL for one repo-relative path
// at an exact revision. Each returns the empty string when it cannot render a
// safe link for the given identity, and the caller then renders the plain
// text instead. A renderer only formats a path, never dereferences it. The
// controller stays source-agnostic through this seam: host and URL shape are
// provider knowledge the wiring supplies, never string literals here.
type WorkItemLinker interface {
	CommitLink(repo, sha string) string
	FileLink(repo, path, sha string) string
}

// resolveIssueNoChangePhase classifies a resolve-issue no_change ending,
// arriving as a trusted native result or as the legacy shim's verified exit.
// A durable, incarnation-fenced grounded close proposal (status.noChange)
// that fences to this pod's UID upgrades the conclusion to
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
	if run.Status.StartedAt != nil {
		// The reported duration belongs to the settlement path exactly as it
		// does to every other lifecycle report; copy the time so the lifecycle
		// never aliases live status.
		t := run.Status.StartedAt.Time
		lifecycle.StartedAt = &t
	}
	if nc := run.Status.NoChange; nc != nil {
		lifecycle.Close = &source.CloseProposal{
			Criteria: toSourceCloseCriteria(nc.Criteria),
			Evidence: nc.Evidence,
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
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseUnsupportedReason, "no grounded close proposal is recorded on the run", source.CloseResolution{})
	}
	reporter, ok := adapter.(source.CloseReporter)
	if !ok {
		// The source offers no evidence-gated close path. It must refuse the
		// result, and the operator must not imitate settlement with an ordinary
		// lifecycle report, which the source contract forbids.
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseUnsupportedReason, "source does not offer an evidence-gated close path", source.CloseResolution{})
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
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, reason, closeResolutionDetail(resolution), resolution)
	case errors.Is(err, source.ErrSuperseded):
		log.FromContext(ctx).Info("dropping superseded already-done close report",
			"run", run.Name, "error", err.Error())
		return ctrl.Result{}, r.markLifecycleReportOutcome(ctx, run, true, lifecycleReportSupersededReason, err.Error(), "", lifecycleReportSupersededReason)
	case errors.Is(err, source.ErrUnsupportedOutcome):
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseUnsupportedReason, err.Error(), source.CloseResolution{})
	case errors.Is(err, source.ErrPermanentRefusal):
		return r.parkAlreadyDoneClose(ctx, run, adapter, item, sourceCloseRefusedPermanentlyReason, err.Error(), source.CloseResolution{})
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
// the parked outcome. The source's real resolution — when the park followed an
// actual close report — is carried through to the condition and the comment,
// so a parked announcement keeps the verification revision the source pinned;
// a zero resolution means no source answer exists, and the detail string is
// reconstructed into a resolution-shaped value in its place.
func (r *CoderRunReconciler) parkAlreadyDoneClose(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, reason, detail string, resolution source.CloseResolution) (ctrl.Result, error) {
	if closeResolutionIsZero(resolution) {
		resolution = closeResolutionFromDetail(detail)
	}
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
// commenter or a delivery failure never alters the run's outcome, but each is
// recorded on the announcement condition as a False status naming why nothing
// was delivered (#727) — a run whose comment could not reach the work item is
// never left reading as announced. The False record is an in-memory mutation
// that rides whichever settlement or transition patch follows. A run whose
// settled outcome was already announced is never announced twice.
func (r *CoderRunReconciler) commentAlreadyDone(ctx context.Context, run *courierv1alpha1.CoderRun, closed bool, reason string, resolution source.CloseResolution) {
	if alreadyDoneCommented(run) {
		return
	}
	if r.WorkItemComments == nil {
		r.recordAnnouncementNotDelivered(run, sourceCloseCommentNotConfiguredReason,
			"the deployment configures no forge comment identity, so this outcome was not announced on the work item")
		return
	}
	body := alreadyDoneCommentBody(run, closed, reason, resolution, r.WorkItemLinks)
	if body == "" {
		return
	}
	if err := r.WorkItemComments.CommentWorkItem(ctx, run, body); err != nil {
		r.recordAnnouncementNotDelivered(run, sourceCloseCommentDeliveryReason, "announcement delivery failed: "+err.Error())
		log.FromContext(ctx).V(1).Info("source-issue comment for already-done run failed",
			"run", run.Name, "error", err.Error())
		return
	}
	// Persist the announce-once marker immediately, through its own patch and
	// again through whichever transition or settlement patch follows: a failed
	// later write must not reopen an unbounded repost loop, so the worst case
	// is the one duplicate a crash between post and marker patch allows.
	before := run.DeepCopy()
	r.markAlreadyDoneCommented(run)
	if err := r.patchStatus(ctx, before, run); err != nil {
		log.FromContext(ctx).V(1).Info("recording the already-done announcement failed",
			"run", run.Name, "error", err.Error())
	}
}

// commentAlreadyDoneHold announces an evidence-incomplete no-change hold on
// the source work item: the run concluded no change without a grounded close
// proposal, so no PR is coming and no closure was attempted. Best-effort and
// once per run, exactly like the settled-outcome notification.
// A missing commenter or a delivery failure is recorded as a False
// announcement status naming why nothing was delivered (#727): the hold's
// explanation reached no one when no identity could post it, and the run must
// not read as announced. The hold runs after its transition patch, so each
// record carries its own patch.
func (r *CoderRunReconciler) commentAlreadyDoneHold(ctx context.Context, run *courierv1alpha1.CoderRun) {
	if alreadyDoneCommented(run) {
		return
	}
	if r.WorkItemComments == nil {
		before := run.DeepCopy()
		r.recordAnnouncementNotDelivered(run, sourceCloseCommentNotConfiguredReason,
			"the deployment configures no forge comment identity, so this hold was not announced on the work item")
		if err := r.patchStatus(ctx, before, run); err != nil {
			log.FromContext(ctx).V(1).Info("recording the undeliverable source-issue hold failed",
				"run", run.Name, "error", err.Error())
		}
		return
	}
	// Name the actual deficiency: nothing grounded, or a proposal left over
	// from a superseded execution whose fence cannot authorize this ending.
	deficiency := "recorded no grounded close evidence"
	if run.Status.NoChange != nil {
		deficiency = "its recorded close proposal belongs to a superseded execution and cannot authorize this ending"
	}
	body := "Courier will not open a pull request for this issue and did not attempt to close it: the run concluded the request was already satisfied but " + deficiency + ", so the conclusion is held for a human. No publication is claimed."
	before := run.DeepCopy()
	if err := r.WorkItemComments.CommentWorkItem(ctx, run, body); err != nil {
		r.recordAnnouncementNotDelivered(run, sourceCloseCommentDeliveryReason, "announcement delivery failed: "+err.Error())
		if err := r.patchStatus(ctx, before, run); err != nil {
			log.FromContext(ctx).V(1).Info("recording the failed source-issue hold comment failed",
				"run", run.Name, "error", err.Error())
		}
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

// recordAnnouncementNotDelivered records in memory that this run's outcome was
// not announced on its source work item, with the reason it could not be:
// no configured comment identity, or a failed delivery. The False status is
// deliberate — a run must never read as announced when no comment can reach
// the work item (#727) — while delivery stays best-effort and the settlement
// unchanged. The caller persists it with the status patch that follows.
func (r *CoderRunReconciler) recordAnnouncementNotDelivered(run *courierv1alpha1.CoderRun, reason, detail string) {
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               sourceCloseCommentedCondition,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            boundedAlreadyDoneText(alreadyDoneCommentRedactor.Redact(detail)),
		LastTransitionTime: metav1.NewTime(r.clock()),
	})
}

// refreshFencedNoChange re-reads the run through the uncached reader and
// adopts a close proposal fenced to the terminating coordinator pod when the
// cached object has not yet caught up with the control incarnation's fenced
// pre-exit write. It reports whether a valid proposal for this incarnation is
// then present, so a cache-lagging reconcile never mistakes a grounded
// conclusion for an evidence-incomplete one.
func (r *CoderRunReconciler) refreshFencedNoChange(ctx context.Context, run *courierv1alpha1.CoderRun, podUID string) bool {
	live := &courierv1alpha1.CoderRun{}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(run), live); err != nil {
		log.FromContext(ctx).V(1).Info("uncached re-read for the fenced no-change proposal failed; holding as evidence-incomplete",
			"run", run.Name, "error", err.Error())
		return false
	}
	if nc := live.Status.NoChange; nc != nil && nc.CoordinatorPodUID == podUID {
		run.Status.NoChange = nc
		return true
	}
	return false
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
// run's close outcome. The structured facts come first: the source-pinned
// verification revision — linked to the forge when the linker can render it —
// and the grounded criteria, each path linked at that exact revision, so a
// hard truncation costs the free-form coordinator assessment, never the links.
// Every non-operator fragment — the coordinator's evidence and criterion text,
// each path, the source's resolution detail, and the evidence digest — passes
// through the redactor before it reaches the forge; constant operator prose
// never does.
func alreadyDoneCommentBody(run *courierv1alpha1.CoderRun, closed bool, reason string, resolution source.CloseResolution, links WorkItemLinker) string {
	var b strings.Builder
	detail := alreadyDoneCommentRedactor.Redact(closeResolutionDetail(resolution))
	if closed {
		b.WriteString("Courier closed this work item: its request is already satisfied by existing work.")
	} else {
		b.WriteString("Courier could not close this work item automatically (" + strings.ToLower(reason) + ")")
		if detail != "" {
			b.WriteString(": " + detail)
		}
		b.WriteString("; a human has been asked to review.")
	}
	// A revision is used only when it is plain hex: as text or as part of a
	// link, anything else could carry markup or credential-shaped text into
	// the comment, so an irregular value is treated as no pinned revision at
	// all — the block and the path links are omitted rather than trusted.
	sha := strings.TrimSpace(resolution.HeadSHA)
	if !isRevisionText(sha) {
		sha = ""
	}
	if sha != "" {
		revision := sha
		if repo := run.Spec.Repo; repo != "" {
			revision = repo + "@" + sha
		}
		if links != nil {
			if link := links.CommitLink(run.Spec.Repo, sha); link != "" {
				revision = "[" + revision + "](" + link + ")"
			}
		}
		b.WriteString(" Verified at " + revision + ".")
		if digest := strings.TrimSpace(resolution.EvidenceDigest); digest != "" {
			b.WriteString(" Evidence catalog: " + alreadyDoneCommentRedactor.Redact(digest) + ".")
		}
	}
	if nc := run.Status.NoChange; nc != nil {
		if len(nc.Criteria) > 0 {
			b.WriteString(" Grounded criteria: ")
			for i, c := range nc.Criteria {
				if i > 0 {
					b.WriteString(", ")
				}
				// Only a path that the redactor returns unchanged may be linked:
				// a redacted path is credential-shaped, and its placeholder must
				// not travel into an href either.
				path := alreadyDoneCommentRedactor.Redact(c.Path)
				if links != nil && sha != "" && path == c.Path {
					if link := links.FileLink(run.Spec.Repo, path, sha); link != "" {
						path = "[" + path + "](" + link + ")"
					}
				}
				b.WriteString(fmt.Sprintf("%s (in %s)", alreadyDoneCommentRedactor.Redact(c.Criterion), path))
			}
		}
		if nc.Evidence != "" {
			b.WriteString(" Coordinator assessment: " + alreadyDoneCommentRedactor.Redact(nc.Evidence) + ".")
		}
	}
	return boundAlreadyDoneComment(b.String())
}

// isRevisionText reports whether a source-reported revision is plain hex
// (either case), the only shape safe to render as announcement text.
func isRevisionText(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return s != ""
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

// closeResolutionIsZero reports whether a resolution carries no source answer
// at all, so a park can substitute the detail string for it.
func closeResolutionIsZero(resolution source.CloseResolution) bool {
	return resolution.Action == "" &&
		resolution.Reason == "" &&
		len(resolution.Reasons) == 0 &&
		resolution.HeadSHA == "" &&
		resolution.EvidenceDigest == ""
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
