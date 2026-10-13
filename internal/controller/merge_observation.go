package controller

import (
	"context"
	"strconv"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
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
// resolve. Everything else keeps the run in AwaitingReview: an open PR, a PR
// closed without merge, a read failure, or a PR whose head identity no
// longer matches what the run recorded are never read as success, and are
// retried at the slow cadence. A run with no recorded pull-request identity
// cannot be observed without inferring from a branch name, so it is left
// alone without a timer rather than polled on a guess.
func (r *CoderRunReconciler) observeMergedReviewPR(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	if r.PullRequests == nil || !r.PullRequests.Capabilities().Available(forge.CapabilityReadPullRequest).Available {
		return ctrl.Result{}, nil
	}
	number, err := strconv.Atoi(strings.TrimSpace(run.Status.PR))
	if err != nil || number <= 0 || strings.TrimSpace(run.Status.Branch) == "" {
		return ctrl.Result{}, nil
	}
	pull, err := r.PullRequests.ReadPullRequest(ctx, forge.PullRequestRef{Repo: run.Spec.Repo, Number: number})
	if err != nil {
		// A forge failure proves nothing about the pull request, so the run
		// keeps its phase; retry at the slow cadence instead of returning an
		// error, which would burn the workqueue's backoff on a healthy run.
		log.FromContext(ctx).V(1).Info("awaiting-review merge observation failed; retrying at the slow cadence",
			"repo", run.Spec.Repo, "pr", run.Status.PR, "error", err.Error())
		return ctrl.Result{RequeueAfter: mergeObservationRequeueDelay}, nil
	}
	if !exactPullRequestMerged(run, number, pull) {
		if pull.Merged {
			// A merged PR that is not this run's exact recorded identity is
			// an anomaly worth one durable line: it means the recorded head
			// drifted, and the run will wait for a merge of its own.
			log.FromContext(ctx).Info("recorded pull request is merged but no longer matches the run's head identity; leaving the run in awaiting-review",
				"repo", run.Spec.Repo, "pr", run.Status.PR, "recordedHeadRepo", run.Status.HeadRepo, "recordedBranch", run.Status.Branch,
				"observedHeadRepo", pull.HeadRepo, "observedBranch", pull.HeadRef)
		}
		return ctrl.Result{RequeueAfter: mergeObservationRequeueDelay}, nil
	}
	log.FromContext(ctx).Info("recorded pull request merged; resolving run",
		"repo", run.Spec.Repo, "pr", run.Status.PR, "branch", run.Status.Branch)
	return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseDone, run.Status.PR, terminalLifecycleIntent{}, nil)
}

// exactPullRequestMerged reports whether the read pull request is the exact
// one the run recorded — same base repository and number, same head
// repository and ref — and the forge reports it merged. An empty
// status.headRepo means a same-repository head, the same convention the
// observer uses for runs persisted before head identity was recorded. A
// changed head identity means the read pull request is not the one this run
// produced, so a merge on it is never this run's completion. Repository
// names compare case-insensitively: GitHub repository names cannot collide
// across case, and the recorded spec repository comes from the source while
// the forge answers the canonical casing, so a casing drift must not stall
// a real merge forever. Head SHA is deliberately not part of the identity: a
// pushed commit repairs review feedback and still ships this run's work, and
// a resolve-issue head is recorded without a SHA.
func exactPullRequestMerged(run *courierv1alpha1.CoderRun, number int, pull forge.PullRequest) bool {
	if !pull.Merged || pull.Number != number {
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
