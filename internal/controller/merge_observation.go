package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/forge"
)

// mergeObservationRequeueDelay is the slow cadence at which a settled
// AwaitingReview run re-reads its recorded pull request for a human merge.
// It is deliberately far slower than the Verifying poll: a retained run wakes
// for one authoritative pull-request read, never for CI-state polling, so the
// observation loop stays bounded no matter how long a PR waits for review.
const mergeObservationRequeueDelay = 5 * time.Minute

// mergeObservedAtAnnotation records, as an RFC3339 timestamp, when the merge
// observation last read the run's recorded pull request. The observation is
// deliberately slow, and the marker keeps it slow: a wake the run's own
// duties cause — a park close the source keeps refusing, a requeued reconcile
// — must never sample the forge faster than the cadence allows. The marker
// lives on the object because process memory dies with the controller, and a
// restart must not turn the slow cadence into a poll.
const mergeObservedAtAnnotation = "courier.misospace.dev/merge-observed-at"

// PullRequestReader is the forge capability seam the post-review merge
// observation reads through: the configured provider's exact pull-request
// read, forge.CapabilityReadPullRequest. The core never speaks a forge API or
// CLI directly, and carrying the capability diagnostic with the read means a
// provider that cannot read a pull request authoritatively is never asked to
// guess one.
type PullRequestReader interface {
	Capabilities() forge.Capabilities
	ReadPullRequest(context.Context, forge.PullRequestRef) (forge.PullRequest, error)
}

// observeMergedReviewPR gives a settled AwaitingReview run one slow, bounded
// world read of the exact pull request identity it recorded: the base
// repository and PR number on status.pr plus the head identity on status. A
// merge the forge confirms on that exact identity ends the work: the run
// transitions to Done, whose existing lifecycle resolves the source before
// the done-at marker starts retention, so deletion still requires the source
// resolve. A pull request the forge reports closed without merge on that exact
// identity is not this run's success either, but it will never become one, so
// the run terminalizes to NeedsHuman with the PullRequestClosedUnmerged
// condition (#277): the source is parked for a human, and the run enters the
// bounded NeedsHuman retention instead of being held in AwaitingReview
// forever. An open PR, an unknown state, a read failure, or a PR whose head
// identity no longer matches what the run recorded are never read as success
// and are retried at the slow cadence; a missing or unreadable PR is never
// treated as closed. A run with no recorded pull-request identity cannot be
// observed without inferring from a branch name, so it is left alone without
// a timer rather than polled on a guess.
func (r *CoderRunReconciler) observeMergedReviewPR(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	if r.PullRequests == nil || !r.PullRequests.Capabilities().Available(forge.CapabilityReadPullRequest).Available {
		return ctrl.Result{}, nil
	}
	number, err := strconv.Atoi(strings.TrimSpace(run.Status.PR))
	if err != nil || number <= 0 || strings.TrimSpace(run.Status.Branch) == "" {
		return ctrl.Result{}, nil
	}
	// The slow cadence is durable. The marker on the run records when the
	// last observation happened, so a wake the run's own duties cause — a
	// park close the source keeps refusing, a requeued reconcile — never
	// samples the forge faster than the cadence allows. A missing or
	// unreadable marker is due: the observation fails open to observing, not
	// to never observing.
	if remaining, ok := r.mergeObservationRemaining(run); ok {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	pull, err := r.PullRequests.ReadPullRequest(ctx, forge.PullRequestRef{Repo: run.Spec.Repo, Number: number})
	if err != nil {
		// A forge failure proves nothing about the pull request, so the run
		// keeps its phase; retry at the slow cadence instead of returning an
		// error, which would burn the workqueue's backoff on a healthy run.
		log.FromContext(ctx).V(1).Info("awaiting-review merge observation failed; retrying at the slow cadence",
			"repo", run.Spec.Repo, "pr", run.Status.PR, "error", err.Error())
		if err := r.stampMergeObserved(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: mergeObservationRequeueDelay}, nil
	}
	if !exactPullRequestIdentity(run, number, pull) {
		if pull.Merged {
			// A merged PR that is not this run's exact recorded identity is
			// an anomaly worth one durable line: it means the recorded head
			// drifted, and the run will wait for a merge of its own.
			log.FromContext(ctx).Info("recorded pull request is merged but no longer matches the run's head identity; leaving the run in awaiting-review",
				"repo", run.Spec.Repo, "pr", run.Status.PR, "recordedHeadRepo", run.Status.HeadRepo, "recordedBranch", run.Status.Branch,
				"observedHeadRepo", pull.HeadRepo, "observedBranch", pull.HeadRef)
		}
		if err := r.stampMergeObserved(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: mergeObservationRequeueDelay}, nil
	}
	if pull.Merged {
		log.FromContext(ctx).Info("recorded pull request merged; resolving run",
			"repo", run.Spec.Repo, "pr", run.Status.PR, "branch", run.Status.Branch)
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseDone, run.Status.PR, terminalLifecycleIntent{}, nil)
	}
	if strings.EqualFold(strings.TrimSpace(pull.State), "closed") {
		// The exact recorded pull request is closed and the forge does not
		// report it merged: it will never complete this run, so park it for a
		// human rather than hold it in AwaitingReview forever. The recorded
		// identity rides on the condition and the source error so an operator
		// can find the closed PR.
		identity := fmt.Sprintf("%s#%d", run.Spec.Repo, number)
		message := fmt.Sprintf("recorded pull request %s was closed without merge", identity)
		log.FromContext(ctx).Info("recorded pull request closed without merge; parking run for a human",
			"repo", run.Spec.Repo, "pr", run.Status.PR, "branch", run.Status.Branch)
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, run.Status.PR, terminalLifecycleIntent{
			error: message,
			conditions: []metav1.Condition{{
				Type:               courierv1alpha1.ConditionPullRequestClosedUnmerged,
				Status:             metav1.ConditionTrue,
				Reason:             courierv1alpha1.ConditionReasonClosedUnmerged,
				Message:            identity,
				ObservedGeneration: run.Generation,
				LastTransitionTime: metav1.NewTime(r.clock()),
			}},
		}, nil)
	}
	// The pull request is still open: the run stays in AwaitingReview, and
	// the observation is marked so the slow cadence survives the wakes the
	// run's own duties cause.
	if err := r.stampMergeObserved(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: mergeObservationRequeueDelay}, nil
}

// mergeObservationRemaining reports how long the slow merge observation must
// still wait before it may read the recorded pull request again, and whether
// it must wait at all. A missing marker, an unreadable one, or one the clock
// has not reached is due: the observation fails open to observing.
func (r *CoderRunReconciler) mergeObservationRemaining(run *courierv1alpha1.CoderRun) (time.Duration, bool) {
	raw, ok := run.Annotations[mergeObservedAtAnnotation]
	if !ok {
		return 0, false
	}
	stampedAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, false
	}
	elapsed := r.clock().UTC().Sub(stampedAt)
	if elapsed < 0 || elapsed >= mergeObservationRequeueDelay {
		return 0, false
	}
	return mergeObservationRequeueDelay - elapsed, true
}

// stampMergeObserved records the durable merge-observation marker on the run.
//
// It is written only on the paths where the observation leaves the run in
// AwaitingReview: a run that transitions to Done or NeedsHuman leaves the
// observation behind, and the marker would be a wasted write. The write is a
// narrow merge patch of the run's own metadata, exactly as the reaper's
// retention markers are written.
func (r *CoderRunReconciler) stampMergeObserved(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	before := run.DeepCopy()
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[mergeObservedAtAnnotation] = r.clock().UTC().Format(time.RFC3339)
	return r.Patch(ctx, run, client.MergeFrom(before))
}

// exactPullRequestIdentity reports whether the read pull request is the exact
// one the run recorded — same base repository and number, same head repository
// and ref. An empty status.headRepo means a same-repository head, the same
// convention the observer uses for runs persisted before head identity was
// recorded. A changed head identity means the read pull request is not the one
// this run produced, so neither a merge nor a close on it is this run's
// outcome. Repository names compare case-insensitively: GitHub repository
// names cannot collide across case, and the recorded spec repository comes
// from the source while the forge answers the canonical casing, so a casing
// drift must not stall a real merge forever. Head SHA is deliberately not part
// of the identity: a pushed commit repairs review feedback and still ships this
// run's work, and a resolve-issue head is recorded without a SHA.
func exactPullRequestIdentity(run *courierv1alpha1.CoderRun, number int, pull forge.PullRequest) bool {
	if pull.Number != number {
		return false
	}
	headRepo := strings.TrimSpace(run.Status.HeadRepo)
	if headRepo == "" {
		headRepo = run.Spec.Repo
	}
	return strings.EqualFold(pull.BaseRepo, run.Spec.Repo) &&
		strings.EqualFold(pull.HeadRepo, headRepo) &&
		pull.HeadRef == run.Status.Branch
}
