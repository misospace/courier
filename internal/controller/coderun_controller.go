package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/executor"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
	"github.com/misospace/courier/internal/topology"
)

// capacityRequeueDelay bounds how long a Pending run can wait behind a full
// lane before checking again.
const (
	capacityRequeueDelay             = 15 * time.Second
	observationRequeueDelay          = 5 * time.Second
	lifecycleReportRequeueDelay      = 30 * time.Second
	provisioningInFlightRequeueDelay = 5 * time.Second

	// defaultProvisioningCollisionGrace is how long a run-owned coordinator
	// pod may stay Terminating while blocking its run's slot before the run
	// escalates to NeedsHuman (#560).
	defaultProvisioningCollisionGrace = 6 * time.Hour
)

// coordinatorContainerName is the coordinator's container name, shared by the
// legacy single-pod topology and the secure topology's control pod.
const coordinatorContainerName = "coordinator"

// lifecycleReportedCondition tracks whether a terminal run's source lifecycle
// report has been published; a False status means a later reconcile retries it.
const (
	lifecycleReportedCondition                              = courierv1alpha1.ConditionLifecycleReported
	provisioningCollisionCondition                          = "ProvisioningCollision"
	provisioningCollisionStuckTerminatingReason             = "StuckTerminatingPod"
	lifecycleReportSupportedCondition                       = "LifecycleReportSupported"
	lifecycleReportPendingReason                            = "Pending"
	lifecycleReportPendingBlockedReportParksPRFixReason     = "PendingBlockedReportParksPRFix"
	lifecycleReportPendingWithErrorReason                   = "PendingWithError"
	lifecycleReportPendingExternalVerificationFailureReason = "PendingExternalVerificationFailure"
	lifecycleReportUnsupportedOutcomeReason                 = "UnsupportedOutcome"
	lifecycleReportPublishedReason                          = "Published"
	lifecycleReportSupersededReason                         = "Superseded"
	lifecycleReportPermanentRefusalReason                   = "PermanentRefusal"
	lifecycleReportRetryableReason                          = "Retryable"
	lifecycleReportAcceptedSupportReason                    = "Accepted"
	retryLifecycleReportAnnotation                          = "courier.misospace.dev/retry-lifecycle-report"
	externalVerificationFailureReason                       = "external verification failed"
)

type terminalLifecycleIntent struct {
	skipPRFixQueueMark bool
	ciFailure          bool
	error              string
	diagnostic         *courierv1alpha1.TerminalDiagnostic
	// conditions are extra status conditions applied in the same status patch
	// as the phase change, so a caller can record the reason for a terminal
	// transition atomically with it (for example a closed-unmerged pull
	// request settling AwaitingReview as NeedsHuman, #277).
	conditions []metav1.Condition
}

// CoderRunReconciler reconciles a CoderRun object.
type CoderRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// APIReader reads the API server directly, bypassing the informer cache.
	// The pod-loss backstop uses it to confirm a missing coordinator really is
	// gone before charging a relaunch: a cache that has not caught up must not
	// be mistaken for infrastructure loss, and infrastructure loss must not be
	// hidden by a cache that never delivers the pod. The lane capacity count —
	// ordinary admission and CI-repair reacquisition alike — reads through it
	// too, so a just-persisted Claimed admission is never missed by a lagging
	// cache (#548). It is required in production — SetupWithManager rejects a
	// nil reader so neither confirmation can fall back to cached state; the
	// reader() fallback serves only direct-construction tests.
	APIReader client.Reader

	// Launch is injected by the coordinator launcher. It is optional while
	// launch support is being assembled; admission still reserves capacity by
	// moving Pending runs to Claimed.
	Launch LaunchFunc

	// Sources resolves the adapter named by CoderRun.Spec.Source. WorkItemID is
	// passed through unchanged for every claim/release/transition call.
	Sources *SourceRegistry

	// PRHeadResolver is required for fix-pr runs. It resolves the PR head —
	// the repository, branch, and commit attached to the existing PR; no
	// branch is synthesized from the PR number.
	PRHeadResolver ExistingPRHeadResolver

	// StatusWriter is the status transport used for operator-owned fields.
	// Nil falls back to a JSON merge patch through the reconciler's client.
	StatusWriter status.PatchWriter

	// Observer reads the external pull request and CI state after a successful run.
	Observer WorldObserver

	// PullRequests, when set, gives a settled AwaitingReview run its slow,
	// bounded world observation: an exact read of the recorded pull request
	// through the configured forge capability. A confirmed merge resolves
	// the run to Done and enters the existing resolve-then-reap lifecycle.
	// Nil means post-review merges are never observed, so the run keeps the
	// watch-only behavior of a deployment without a forge read path.
	PullRequests PullRequestReader

	// Evidence reconciles the run's failure-evidence Secrets at the terminal
	// boundary the operator can prove: a world-verified success (AwaitingReview,
	// Done) has its evidence deleted, and a dirty terminal (Failed, NeedsHuman)
	// run gains the informational EvidenceCaptured condition when evidence is
	// present in the world. It never touches the phase and is a no-op when nil;
	// production wires it to the controller-runtime-managed EvidenceIntake.
	Evidence *EvidenceIntake

	// Events optionally emits structured run events as JSON lines for the
	// deployment's log collection stack. Nil disables emission; a failed
	// emission never fails reconciliation.
	Events *courierlog.Emitter

	// DoneRetention is how long a resolved Done run is kept before deletion.
	// Zero uses the package default.
	DoneRetention time.Duration

	// FailedRetention is how long a Failed run is kept before deletion, once
	// its lifecycle report has reached a positive, settled state. Zero uses the
	// package default.
	FailedRetention time.Duration

	// ProvisioningCollisionGrace is how long a run-owned coordinator pod may
	// stay Terminating while blocking this run's slot before the run escalates
	// to NeedsHuman (#560). Zero uses the package default.
	ProvisioningCollisionGrace time.Duration

	// NeedsHumanRetention is how long a NeedsHuman run is kept before deletion,
	// once its lifecycle report has reached a positive, settled state. Zero
	// uses the package default.
	NeedsHumanRetention time.Duration

	// TerminalAuditKeep is the number of most recent terminal runs per
	// repository the reaper always retains, even past their retention window.
	// Zero uses the package default; a negative value disables the floor.
	TerminalAuditKeep int

	// LivenessWindow is how long a Running run may go without a heartbeat before
	// it is considered wedged and reaped. Zero uses the package default.
	LivenessWindow time.Duration

	// MaxRestarts is the crashloop threshold: after this many infra relaunches a
	// run transitions to NeedsHuman. Zero uses the package default.
	MaxRestarts int

	// CapacityCooldownWindow is how long a capacity identity stays paused for
	// new work after a confirmed execution-capacity exhaustion (#327). It is
	// the operator's bounded re-claim control — never a provider-observed
	// reset time, which Courier does not invent — and it never bounds a run
	// already executing. Zero uses the package default.
	CapacityCooldownWindow time.Duration

	// Now is the reconciler's time source, injectable so tests never sleep.
	// Nil falls back to time.Now.
	Now func() time.Time

	// Metrics records terminal-phase observations (run duration, queue wait,
	// totals). Nil falls back to the process-default recorder on the
	// controller-runtime metrics registry.
	Metrics *RunRecorder

	// Secure, when set, routes runs through the isolated control/broker/
	// worker topology instead of the legacy single-pod coordinator. It owns
	// admission policy resolution, preflight, provisioning, and revocation.
	// Nil means the deployment runs legacy mode only, which stays explicitly
	// insecure.
	Secure *SecureControl
}

// metrics returns the run recorder to use, defaulting to the process-wide
// recorder on the controller-runtime metrics registry.
func (r *CoderRunReconciler) metrics() *RunRecorder {
	if r.Metrics != nil {
		return r.Metrics
	}
	return processRunRecorder()
}

// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=laneprofiles,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=laneprofiles/status,verbs=get;patch;update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete

// Reconcile is the CoderRun control loop.
//
// Pending runs are admitted against the lane's current Claimed + Running
// count. Claiming happens before launch so the claim itself reserves capacity;
// a successful coordinator exits to Verifying, which does not reserve capacity;
// a failed launch releases its reservation and leaves the run retryable.
// secureTopologyFinalizer is the CoderRun finalizer for ordered revocation of
// the per-run secure topology.
const secureTopologyFinalizer = topology.FinalizerName

func (r *CoderRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var run courierv1alpha1.CoderRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A deleting secure run finishes ordered revocation before its finalizer
	// is released; legacy runs have no topology to revoke.
	if !run.DeletionTimestamp.IsZero() && r.Secure != nil &&
		controllerutil.ContainsFinalizer(&run, secureTopologyFinalizer) {
		done, err := r.Secure.Revoke(ctx, &run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
		}
		if err := r.Secure.RemoveFinalizer(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Evidence lifecycle runs for every non-deleting run ahead of the phase
	// dispatch, so a terminal run the dispatch leaves untouched still has its
	// evidence reconciled. It is a no-op when the intake is disabled or the
	// phase carries no evidence action; a transient failure requeues.
	if err := r.reconcileEvidence(ctx, &run); err != nil {
		return ctrl.Result{}, err
	}

	// Source-facing park reports the run still owes — the park notice, the
	// fenced outcome, and the fence-less deferral release — are durable retry
	// intent. Neither close may preempt the run's own progression or terminal
	// obligations. Pending closes are both attempted on each reconcile, then
	// share the bounded retry from #557.
	if run.Status.CIParkDeferral != nil {
		switch run.Status.Phase {
		case courierv1alpha1.PhaseVerifying, courierv1alpha1.PhaseClaimed, courierv1alpha1.PhaseRunning:
			if delivered, err := r.deliverCIParkNotice(ctx, &run); !delivered {
				log.FromContext(ctx).V(1).Info("source park notice not delivered", "run", run.Name, "error", err.Error())
			}
		default:
			before := run.DeepCopy()
			run.Status.CIParkDeferral = nil
			if err := r.patchStatus(ctx, before, &run); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	closeRetry := time.Duration(0)
	closePending := false
	if run.Status.CISettlement != nil {
		if delivered, err := r.deliverCISettlement(ctx, &run); !delivered {
			log.FromContext(ctx).V(1).Info("source repair settlement not delivered", "run", run.Name, "error", err.Error())
			closePending = true
		}
	}
	if run.Status.CIParkRelease != nil {
		if delivered, err := r.deliverCIParkRelease(ctx, &run); !delivered {
			log.FromContext(ctx).V(1).Info("source deferral release not delivered", "run", run.Name, "error", err.Error())
			closePending = true
		}
	}
	if closePending {
		switch run.Status.Phase {
		case courierv1alpha1.PhaseVerifying, courierv1alpha1.PhaseClaimed, courierv1alpha1.PhaseRunning:
		case courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseHandedOff,
			courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseBlockedExternal:
			closeRetry = ciparkSettlementRetryDelay
		default:
			return ctrl.Result{RequeueAfter: ciparkSettlementRetryDelay}, nil
		}
	}

	if run.Status.Phase == courierv1alpha1.PhaseRunning {
		return r.observeRunning(ctx, &run)
	}
	if run.Status.Phase == courierv1alpha1.PhaseVerifying {
		return r.observeVerifying(ctx, &run)
	}
	if run.Status.Phase == courierv1alpha1.PhaseClaimed {
		return r.resumeClaimed(ctx, &run)
	}
	if run.Status.Phase == courierv1alpha1.PhaseDone {
		return r.resolveCompleted(ctx, &run)
	}
	// Status is initially empty on a newly-created CoderRun. Treat that zero
	// value as Pending so a source does not need a second status write before
	// admission can begin. A terminal run normally does nothing, except when its
	// lifecycle report is still pending: then the report alone is retried, since
	// the run already reached its phase and freed capacity. An AwaitingReview
	// run whose LifecycleReported condition has reached a positive, settled
	// state (Status=True with reason Published, Superseded, or
	// PermanentRefusal) additionally takes
	// one slow, bounded read of its recorded pull request, so a human merge can
	// finish and reap it.
	if run.Status.Phase != "" && run.Status.Phase != courierv1alpha1.PhasePending {
		result, err := r.reconcileTerminal(ctx, &run)
		if err != nil {
			if closeRetry == 0 {
				return ctrl.Result{}, err
			}
			// The close the source keeps refusing still owes a bounded
			// retry, and the workqueue's rate-limited backoff on this error
			// can outlast that bound: the error's own delay would push the
			// close's re-presentation past the settlement cadence. The close
			// owns the wake instead — the failure is logged and the
			// reconcile returns at the cadence, so the close is never
			// re-presented later than the bound allows. A run that owes no
			// close keeps the error, and with it the workqueue's backoff.
			log.FromContext(ctx).Error(err, "terminal dispatch failed; retrying at the settlement cadence",
				"run", run.Name, "phase", run.Status.Phase)
			return capCloseRetry(result, closeRetry), nil
		}
		return capCloseRetry(result, closeRetry), nil
	}

	adapter, item, err := r.adapterAndWorkItem(&run)
	if err != nil {
		return ctrl.Result{}, err
	}

	var lane courierv1alpha1.LaneProfile
	if err := r.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, &lane); err != nil {
		return ctrl.Result{}, err
	}
	// A suspended lane admits nothing new. Only Pending runs reach this point,
	// so runs already claimed or running finish normally. Requeue like a full
	// lane, since lifting the annotation does not reconcile Pending runs.
	if lane.Suspended() {
		l.V(1).Info("lane suspended, run waits", "lane", run.Spec.Lane)
		return ctrl.Result{RequeueAfter: capacityRequeueDelay}, nil
	}
	// An active capacity cooldown on this lane's capacity identity withholds
	// admission too (#327): discovery is paused on the runner, but runs the
	// runner materialized before the exhaustion must not be launched straight
	// back into exhausted capacity. Runs already admitted (Claimed, Running)
	// are never touched — the cooldown bounds re-claim, not execution.
	var lanes courierv1alpha1.LaneProfileList
	if err := r.List(ctx, &lanes, client.InNamespace(run.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	if cooldown, active := courierv1alpha1.CapacityCooldownActive(&lane, lanes.Items, r.clock()); active {
		l.V(1).Info("capacity cooldown active, run waits",
			"lane", run.Spec.Lane,
			"capacityID", lane.CapacityIdentity(),
			"nextEligibleAt", cooldown.NextEligibleAt.Time)
		return ctrl.Result{RequeueAfter: capacityRequeueDelay}, nil
	}

	// The admitted count goes through the uncached reader: a cached List can
	// lag a sibling run's just-persisted Claimed admission by the informer's
	// relist window, and a lagging count lets a competing run claim the final
	// slot of a concurrency-1 lane (#548).
	var runs courierv1alpha1.CoderRunList
	if err := r.reader().List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	if !laneHasCapacity(runs.Items, run.Spec.Lane, lane.Spec.Concurrency) {
		l.V(1).Info("lane at capacity",
			"lane", run.Spec.Lane,
			"capacity", lane.Spec.Concurrency,
			"admitted", admittedCount(runs.Items, run.Spec.Lane),
		)
		// No object transition wakes a Pending run when capacity frees: the
		// run that releases a slot only triggers its own reconcile. Requeue
		// so the wait is bounded rather than permanent; the delay also covers
		// a concurrency bump on the LaneProfile itself.
		return ctrl.Result{RequeueAfter: capacityRequeueDelay}, nil
	}

	if claimer, ok := adapter.(source.ClaimerWithReceipt); ok {
		claimed, err := claimer.ClaimWithReceipt(ctx, item)
		if err != nil {
			return ctrl.Result{}, err
		}
		if claimed.Context != "" && len(claimed.Context) > source.MaxSourceContextBytes {
			return ctrl.Result{}, fmt.Errorf("source claim returned oversized receipt")
		}
		item.Context = claimed.Context
	} else if err := adapter.Claim(ctx, item); err != nil {
		return ctrl.Result{}, err
	}
	// Claimed is the admission reservation. Persist it before launching so a
	// concurrent reconciliation cannot observe an unaccounted-for launch.
	before := run.DeepCopy()
	run.Status.Phase = courierv1alpha1.PhaseClaimed
	if _, ok := adapter.(source.ClaimerWithReceipt); ok {
		run.Status.SourceReceipt = item.Context
	}
	if run.Status.AdmittedAt == nil {
		run.Status.AdmittedAt = &metav1.Time{Time: r.clock().UTC()}
	}
	if err := r.patchStatus(ctx, before, &run); err != nil {
		return ctrl.Result{}, errors.Join(err, adapter.Release(ctx, item))
	}

	head, err := resolveRunBranch(ctx, &run, r.PRHeadResolver)
	if err != nil {
		if errors.Is(err, ErrHeadRepositoryGone) {
			return r.terminateMissingHead(ctx, &run, err)
		}
		return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
	}
	before = run.DeepCopy()
	run.Status.Branch = head.Branch
	run.Status.HeadRepo = head.Repo
	run.Status.HeadSHA = head.SHA
	if err := r.patchStatus(ctx, before, &run); err != nil {
		return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
	}
	if r.Launch != nil {
		if err := preLaunch(ctx, adapter, item); err != nil {
			return ctrl.Result{}, r.rejectClaim(ctx, &run, adapter, item, err)
		}
	}

	// Secure admission resolves and persists the immutable publication policy
	// before any topology is provisioned: selection is fail-closed, and a
	// permanent admission failure terminalizes the run instead of cycling
	// claim and release.
	if r.Secure != nil {
		if err := r.resolvePersistSecurePolicy(ctx, &run); err != nil {
			if permanent := (*SecureNeedsHumanError)(nil); errors.As(err, &permanent) {
				return r.Secure.transitionNeedsHuman(ctx, &run, permanent.Detail)
			}
			return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
		}
	}

	launched := r.Launch != nil
	if launched {
		if err := adapter.Transition(ctx, item, source.StateInProgress); err != nil {
			return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
		}
		beforeLaunch := run.DeepCopy()
		if r.Secure != nil {
			result, err := r.Secure.LaunchSecure(ctx, &run)
			if err != nil {
				return r.secureLaunchFailure(ctx, &run, adapter, item, err)
			}
			if result.Requeue || result.RequeueAfter > 0 {
				// Provisioning is in flight; the run stays Claimed and the
				// next reconcile resumes it.
				return result, nil
			}
		} else if err := r.Launch(ctx, &run); err != nil {
			if permanent := launchRejection(err); permanent != nil {
				return r.terminateNeedsHuman(ctx, &run, adapter, item, permanent)
			}
			if errors.Is(err, ErrProvisioningInFlight) {
				var collision *ProvisioningCollisionError
				if errors.As(err, &collision) {
					return r.terminateStuckTerminatingCollision(ctx, &run, adapter, item, collision)
				}
				// The deterministic pod name could not be verified as a
				// live coordinator pod owned by this run: it is being
				// torn down, unreadable, or owned by something else
				// (including a dead incarnation of the same name).
				// Writing Running over that slot would bind the run to a
				// dying or foreign coordinator — the next reconcile
				// could terminalize it as Failed off the pod's own
				// termination, and the disappearance path would charge a
				// second infrastructure loss if the pod vanishes before
				// the wedge path observes it. Stay Claimed and let the
				// pod delete event wake the next reconcile; the bounded
				// requeue is the backstop.
				l.V(1).Info("coordinator slot not verifiably a live run-owned pod; staying Claimed",
					"run", run.Name, "pod", run.Name)
				return ctrl.Result{RequeueAfter: provisioningInFlightRequeueDelay}, nil
			}
			return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
		}
		run.Status.Phase = courierv1alpha1.PhaseRunning
		if err := r.patchStatus(ctx, beforeLaunch, &run); err != nil {
			return ctrl.Result{}, err
		}
		r.emitPhaseTransition(&run, courierv1alpha1.PhaseRunning, map[string]any{"branch": run.Status.Branch, "lane": run.Spec.Lane})
	}

	l.V(1).Info("run admitted", "phase", run.Status.Phase,
		"mode", run.Spec.Mode,
		"repo", run.Spec.Repo,
		"ref", run.Spec.Ref,
		"lane", run.Spec.Lane,
	)
	return ctrl.Result{}, nil
}

// reconcileTerminal owns the terminal-phase dispatch of a run that already
// reached its phase and freed capacity.
//
// The terminal run is not finished: it still owes the source its lifecycle
// report, its slow merge observation, its retention reap, or its capacity
// recovery. None of those is preempted by the run's other durable duties — a
// park close the source keeps refusing rides along as a cap on the reconcile's
// wake interval, never as an early return in front of this dispatch — so the
// report retry and the merge observation always get their turn. The one early
// return is the #564 fence backfill below, which holds the dispatch for one
// settlement cadence and no more. A Done run never reaches this dispatch (the
// resolveCompleted path owns it) and cannot carry a live attempt: every path
// to Done grades the in-flight attempt through publication observation or the
// terminal write, each of which settles or clears it.
func (r *CoderRunReconciler) reconcileTerminal(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	// A run that terminalized before the terminal write began settling an
	// in-flight repair attempt can still carry a live fence, and the source
	// holds no expiry on an active deferral: it would defer its hand-out
	// against a run that can never report. Settle the fence the way
	// transitionTerminal now does, and wake at the settlement cadence so the
	// recorded close is delivered — from the patch on, the ordinary close
	// delivery ahead of this dispatch owns every later retry. The
	// abandonment is idempotent: once patched the attempt is nil and this
	// path never fires again (#564).
	if run.Status.CIRepair != nil {
		before := run.DeepCopy()
		abandonCIRepairAttempt(run)
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
		log.FromContext(ctx).Info("terminal run still held a repair fence; settled it as owed",
			"run", run.Name, "phase", run.Status.Phase)
		if run.Status.CISettlement != nil {
			return ctrl.Result{RequeueAfter: ciparkSettlementRetryDelay}, nil
		}
	}
	// The explicit retry request surface (#97/#606) is translated at the head of
	// the terminal duties, so an operator's authorized retry converges before the
	// run's ordinary retention, report, or recovery work. It never changes the
	// phase, never deletes, and never touches evidence: it only reads/writes
	// status.retryRequest and, for an eligible run, calls the source's fenced
	// requeue capability. It is a no-op (handled=false) unless an annotation names
	// a request or a record is outstanding, so ordinary terminal runs are
	// unaffected.
	retryResult, retryHandled, retryErr := r.syncRetry(ctx, run)
	if retryErr != nil {
		// A retry write or read that failed surfaces the error so the workqueue
		// re-arms and re-drives the durable intent; the announce-over-Requeued patch
		// returns handled=false with an error specifically so a lost metadata write
		// re-announces rather than dropping the audit signal.
		return ctrl.Result{}, retryErr
	}
	if retryHandled {
		return retryResult, nil
	}
	// Capacity availability is independent of source reporting. Persist it
	// before a pending or unsupported lifecycle report can return early.
	if isCapacityExhaustionRun(run) {
		if err := r.recordCapacityCooldown(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if lifecycleReportPending(run) {
		if lifecycleReportUnsupported(run) {
			if lifecycleReportRetryTokenInvalid(run) {
				log.FromContext(ctx).Info("ignoring invalid lifecycle retry token; token must be non-empty and at most 128 Unicode code points")
			}
			if !lifecycleReportRetryRequested(run) {
				return ctrl.Result{}, nil
			}
		}
		return r.retryTerminalLifecycle(ctx, run)
	}
	if run.Status.Phase == courierv1alpha1.PhaseAwaitingReview {
		// The pending-report retry above handles a False LifecycleReported.
		// A missing, Unknown, or True-with-another-reason condition does not
		// prove the source saw the in-review lifecycle either, so the
		// post-review merge observation must wait too: a legacy or partially
		// upgraded AwaitingReview run without a positively settled
		// LifecycleReported (Published, Superseded, or PermanentRefusal)
		// cannot transition to Done without first proving its in-review
		// lifecycle. The run stays
		// parked without a timer; the report-retry path above re-arms any
		// pending report, and a run whose condition is missing/Unknown/other
		// remains unobserved until the source publishes the report or an
		// operator intervenes.
		if !lifecycleReportSettled(run) {
			log.FromContext(ctx).V(1).Info("awaiting-review run observed without a positively settled LifecycleReported; leaving in awaiting-review",
				"lifecycleReported", lifecycleReportConditionState(run))
			return ctrl.Result{}, nil
		}
		return r.observeMergedReviewPR(ctx, run)
	}
	if (run.Status.Phase == courierv1alpha1.PhaseFailed || run.Status.Phase == courierv1alpha1.PhaseNeedsHuman) && !isCapacityExhaustionRun(run) {
		// A Failed or NeedsHuman run is reaped only once its source
		// lifecycle report reached a positive, settled state: while the
		// report is pending the run is the only durable retry intent, and a
		// missing, Unknown, or False condition proves nothing. Such a run
		// is parked without a timer; the report-retry path above re-arms a
		// pending report, and an operator settles a missing condition.
		if !lifecycleReportSettled(run) {
			log.FromContext(ctx).V(1).Info("terminal run observed without a positively settled LifecycleReported; leaving for retention",
				"phase", run.Status.Phase, "lifecycleReported", lifecycleReportConditionState(run))
			return ctrl.Result{}, nil
		}
		if run.Status.CISettlement != nil || run.Status.CIParkRelease != nil || retryHoldsReap(run) {
			// The run still owes the source a park close, or an explicit retry
			// request is still Requested. Deleting it would drop that durable
			// intent with the object, so the retention window starts only once the
			// owed close lands or the retry record settles — the same posture a
			// Done run keeps while its close is outstanding. A pending close is
			// cleared only by a delivery the source accepted or a refusal proving
			// it moot; a retry record settles through syncRetry, which drives it
			// and holds the run while Requested, so this gate is the reap-decision
			// backstop for that invariant.
			log.FromContext(ctx).V(1).Info("terminal run still owes durable source intent; leaving it for retention",
				"run", run.Name, "phase", run.Status.Phase)
			return ctrl.Result{}, nil
		}
		if run.Status.Phase == courierv1alpha1.PhaseFailed {
			return r.reapTerminal(ctx, run, failedAtAnnotation, r.failedRetention(), true)
		}
		return r.reapTerminal(ctx, run, needsHumanAtAnnotation, r.needsHumanRetention(), true)
	}
	if isCapacityExhaustionRun(run) && capacityRecoveryPending(run) {
		// A capacity settlement left retryable (transient source error or
		// writers still draining) is durable intent on the run: an
		// operator restart re-derives it from the condition and retries
		// here, after the lifecycle report itself has settled.
		adapter, item, err := r.adapterAndWorkItem(run)
		if err != nil {
			return ctrl.Result{}, err
		}
		return r.recoverCapacityExhaustion(ctx, run, adapter, item)
	}
	log.FromContext(ctx).V(1).Info("reconcile", "phase", run.Status.Phase)
	return ctrl.Result{}, nil
}

// capCloseRetry bounds a terminal reconcile's wake interval by the park
// close's bounded retry. The terminal path's own wake wins whenever it is
// sooner, and the close is never re-presented later than the settlement
// cadence allows. A wake of zero means the terminal path asked for no timer
// of its own, so the close's cadence supplies the wake.
func capCloseRetry(result ctrl.Result, closeRetry time.Duration) ctrl.Result {
	if closeRetry > 0 && (result.RequeueAfter == 0 || result.RequeueAfter > closeRetry) {
		result.RequeueAfter = closeRetry
	}
	return result
}

// resumeClaimed closes the small crash/conflict window between reserving a run
// and recording that its pod is Running. Launch is idempotent at the Kubernetes
// resource boundary (AlreadyExists is accepted by CoordinatorLauncher), so a
// retry can safely finish a partially-completed admission. The
// ErrProvisioningInFlight sentinel that Launch may return when the slot
// cannot be verified as a live pod owned by this run's UID is the carve-out:
// the resource boundary is no longer authoritative, the slot is being torn
// down or holds a foreign pod, and writing Running over it would bind the
// run to a dying or foreign coordinator (#217).
func (r *CoderRunReconciler) resumeClaimed(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if r.Launch == nil {
		return ctrl.Result{}, nil
	}
	if strings.TrimSpace(run.Status.Branch) == "" {
		head, err := resolveRunBranch(ctx, run, r.PRHeadResolver)
		if err != nil {
			if errors.Is(err, ErrHeadRepositoryGone) {
				return r.terminateMissingHead(ctx, run, err)
			}
			return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
		}
		before := run.DeepCopy()
		run.Status.Branch = head.Branch
		run.Status.HeadRepo = head.Repo
		run.Status.HeadSHA = head.SHA
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
		}
	}
	if err := preLaunch(ctx, adapter, item); err != nil {
		return ctrl.Result{}, r.rejectClaim(ctx, run, adapter, item, err)
	}
	if err := adapter.Transition(ctx, item, source.StateInProgress); err != nil {
		return ctrl.Result{}, err
	}
	beforeLaunch := run.DeepCopy()
	if r.Secure != nil {
		result, err := r.Secure.LaunchSecure(ctx, run)
		if err != nil {
			return r.secureLaunchFailure(ctx, run, adapter, item, err)
		}
		if result.Requeue || result.RequeueAfter > 0 {
			return result, nil
		}
	} else if err := r.Launch(ctx, run); err != nil {
		if permanent := launchRejection(err); permanent != nil {
			return r.terminateNeedsHuman(ctx, run, adapter, item, permanent)
		}
		if errors.Is(err, ErrProvisioningInFlight) {
			var collision *ProvisioningCollisionError
			if errors.As(err, &collision) {
				return r.terminateStuckTerminatingCollision(ctx, run, adapter, item, collision)
			}
			// A relaunch's create collided with a slot that could not be
			// verified as a live pod owned by this run's UID: torn down,
			// unreadable, or foreign. The next reconcile either rides the
			// pod's delete event (the pod owns the watch) or the bounded
			// requeue. Recording Running would bind the run to a dying or
			// foreign coordinator — the wedge path could terminalize the
			// run as Failed off the pod's own termination, and a
			// vanishing pod would charge a second infrastructure loss
			// (#217).
			l.V(1).Info("coordinator slot not verifiably a live run-owned pod; staying Claimed",
				"run", run.Name)
			return ctrl.Result{RequeueAfter: provisioningInFlightRequeueDelay}, nil
		}
		return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
	}
	run.Status.Phase = courierv1alpha1.PhaseRunning
	if err := r.patchStatus(ctx, beforeLaunch, run); err != nil {
		return ctrl.Result{}, err
	}
	r.emitPhaseTransition(run, courierv1alpha1.PhaseRunning, map[string]any{"branch": run.Status.Branch, "lane": run.Spec.Lane})
	return ctrl.Result{}, nil
}

// resolvePersistSecurePolicy resolves the run's immutable publication policy
// once and persists it set-once. A persisted policy is never rewritten: a
// later reconcile re-verifies it against the live run incarnation instead.
func (r *CoderRunReconciler) resolvePersistSecurePolicy(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	if run.Status.PublicationPolicy != nil {
		policy, err := VerifyPersistedPolicy(run)
		if err != nil {
			return secureNeedsHuman("PolicyCorrupt", "%v", err)
		}
		// Re-verify the registry still selects the same registration with
		// the same projection: a changed registry applies to future
		// admissions only, and this run's policy must stay coherent with it.
		registration, err := r.Secure.Config.Registry.Select(run.Spec.Repo)
		if err != nil || registration.Name != policy.ProviderConfigRef || registration.Endpoint != policy.ProviderEndpoint || registration.Projection().Digest != policy.CredentialRefDigest {
			return secureNeedsHuman("PolicyConflict",
				"the provider registry no longer matches this run's persisted policy")
		}
		// A pinned criterion set is re-verified like every other policy
		// field: a digest that no longer covers the pinned criteria is a
		// fail-closed corruption signal, not an input to launch-loop on.
		if _, _, err := broker.CriterionSetFromPolicy(policy); err != nil {
			return secureNeedsHuman("PolicyCorrupt", "%v", err)
		}
		return nil
	}
	_, policy, err := r.Secure.ResolveAdmission(ctx, run)
	if err != nil {
		return err
	}
	before := run.DeepCopy()
	run.Status.PublicationPolicy = policy
	if err := r.patchStatus(ctx, before, run); err != nil {
		return err
	}
	return nil
}

// SetupWithManager registers the controller with the manager.
func (r *CoderRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		return errors.New("coderun controller: APIReader is required so the pod-loss confirmation and the lane capacity count read the API server directly and never fall back to cached state")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&courierv1alpha1.CoderRun{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// emitPhaseTransition writes one phase.transition run event. Verbose detail
// follows the run's own spec.debug flag — carried per event, not by a
// process-global switch — so one run's debug setting never changes another
// run's event verbosity. Emission is best-effort: a nil emitter or a write
// failure never affects reconciliation, and the emitter drops events that
// lack run identity.
func (r *CoderRunReconciler) emitPhaseTransition(run *courierv1alpha1.CoderRun, phase courierv1alpha1.Phase, detail map[string]any) {
	if r == nil || r.Events == nil || run == nil {
		return
	}
	_ = r.Events.Emit(courierlog.Event{
		Type:    courierlog.EventPhaseTransition,
		RunID:   run.Name,
		Repo:    run.Spec.Repo,
		Ref:     run.Spec.Ref,
		Mode:    string(run.Spec.Mode),
		Status:  string(phase),
		Verbose: run.Spec.Debug,
		Detail:  detail,
	})
}

func (r *CoderRunReconciler) adapterAndWorkItem(run *courierv1alpha1.CoderRun) (source.Adapter, source.WorkItem, error) {
	adapter, err := r.Sources.lookupRun(run)
	if err != nil {
		return nil, source.WorkItem{}, err
	}
	item, err := sourceWorkItem(run)
	if err != nil {
		return nil, source.WorkItem{}, err
	}
	return adapter, item, nil
}

func preLaunch(ctx context.Context, adapter source.Adapter, item source.WorkItem) error {
	guard, ok := adapter.(source.PreLauncher)
	if !ok {
		return nil
	}
	return guard.PreLaunch(ctx, item)
}

// rejectClaim rolls back a failed admission. ErrStaleWork is terminal: the
// source has marked the work stale, so the run is deleted instead of being
// left Pending where it would be re-admitted into the same rejection. Any
// other cause leaves the run retryable at Pending.
// A repair attempt in flight survives a retryable rollback intact (#547).
// The context carries the repair goal the relaunch renders, its bounded
// evidence, and the fence token of the reservation the source still holds —
// the attempt is deferred, not over. Clearing it here would drop the repair
// goal (the re-admitted run would run the original task), bypass the
// no-progress guard (which reads the attempt in flight), and let an
// unchanged attempt settle a red-CI pull request onto review. Only the
// park loop's documented endings — finishCIPark, an observed republish or
// no-progress, and abandonCIRepairAttempt on terminalization — end the
// attempt and settle its fence.
func (r *CoderRunReconciler) rejectClaim(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, cause error) error {
	releaseErr := adapter.Release(ctx, item)
	if errors.Is(cause, source.ErrStaleWork) {
		// No settlement is recorded here: the source itself has invalidated
		// the work — its settlement contract refuses outcome reports on
		// stale work (classifyCIRefusal maps those refusals to
		// ErrStaleWork) — and a deleted run has no status to carry durable
		// retry intent.
		deleteErr := r.Delete(ctx, run)
		return errors.Join(cause, releaseErr, deleteErr)
	}
	before := run.DeepCopy()
	run.Status.Phase = courierv1alpha1.PhasePending
	run.Status.Branch = ""
	run.Status.HeadRepo = ""
	run.Status.HeadSHA = ""
	statusErr := r.patchStatus(ctx, before, run)
	return errors.Join(cause, releaseErr, statusErr)
}

// releaseClaim rolls a retryable launch failure back to Pending, attempting
// both sides even if one fails. Returning the joined error keeps the
// original launch/branch failure visible while exposing a stranded source
// claim or status update to the controller. A repair attempt in flight, if
// any, survives intact exactly as rejectClaim leaves it (#547): the attempt
// is deferred, its reservation stays held at the source for the outcome the
// parked loop will eventually report, and the relaunch renders the same
// repair contract.
func (r *CoderRunReconciler) releaseClaim(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, cause error) error {
	// Attempt both sides of the rollback even if one fails. Returning the
	// joined error keeps the original launch/branch failure visible while
	// exposing a stranded source claim or status update to the controller.
	releaseErr := adapter.Release(ctx, item)
	before := run.DeepCopy()
	run.Status.Phase = courierv1alpha1.PhasePending
	run.Status.Branch = ""
	run.Status.HeadRepo = ""
	run.Status.HeadSHA = ""
	statusErr := r.patchStatus(ctx, before, run)
	return errors.Join(cause, releaseErr, statusErr)
}

// abandonCIRepairAttempt ends a repair attempt that was still in flight when
// the run terminalized for a reason the park loop never graded — a permanent
// launch rejection, a missing head, a wedged coordinator, or an ordinary
// path that ended the run outside the loop. The attempt's fence must not
// outlive the run: an unsettled reservation would defer the source's
// hand-out against a run that can never report, so the abandonment is
// recorded on the terminal status write as durable retry intent, and the
// reap gate that blocks deletion while a close is owed covers it like every
// other settlement. Result unsupported names an attempt that ended without
// a documented ending and carries no head — the vocabulary finishCIPark
// records for the non-repushed endings.
//
// A fenced in-flight attempt overwrites a settlement the run already owes:
// the record carries one owed close, and a source grants a later reservation
// only after the earlier outcome committed (a deferral with a held fence
// refuses the next reservation), so the owed record is already moot at the
// source while the live fence is not. An unfenced context is cleared without
// touching an owed close: there is no grant to answer.
func abandonCIRepairAttempt(run *courierv1alpha1.CoderRun) {
	if run.Status.CIRepair == nil {
		return
	}
	if run.Status.CIRepair.FenceToken != "" {
		run.Status.CISettlement = &courierv1alpha1.CISettlement{
			FenceToken:     run.Status.CIRepair.FenceToken,
			IdempotencyKey: ciparkRepairKey(run, run.Status.CIRepair.Attempt),
			Result:         courierv1alpha1.CIParkOutcomeUnsupported,
		}
	}
	run.Status.CIRepair = nil
}

// terminateMissingHead ends a fix-pr run whose pull request head repository is
// gone (a deleted fork). Unlike a transient resolution failure, the condition
// cannot recover by re-claiming, so the claim is released and the run
// terminalizes NeedsHuman rather than cycling claim and release on every
// reconcile. This is the one path where the branch identity is cleared too:
// the head it names no longer exists, and the terminal PR enrichment reads a
// branch to observe, so leaving it set would send a fresh observation at a ref
// that cannot resolve. The cause is handled here, so it is logged and not
// returned as a reconcile error.
func (r *CoderRunReconciler) terminateMissingHead(ctx context.Context, run *courierv1alpha1.CoderRun, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("terminating fix-pr run: pull request head repository is missing",
		"run", run.Name, "branch", run.Status.Branch, "cause", cause.Error())
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	run.Status.Branch = ""
	run.Status.HeadRepo = ""
	run.Status.HeadSHA = ""
	return r.terminateNeedsHuman(ctx, run, adapter, item, &SecureNeedsHumanError{Reason: "HeadRepositoryGone", Detail: cause.Error()})
}

// secureLaunchFailure handles an error from LaunchSecure. The terminal
// verdict classes — a rendered pod spec the API server refuses outright, and
// a probe pod pinned to a node its name does not name — terminalize the run,
// and fence the round first: a resumed launch reaches the launcher with a
// live worker and a credentialed broker already provisioned, and a
// NeedsHuman run is no longer observed and is reaped only after its retention
// window, so a fence deferred past this reconcile would leave the untrusted
// executor running and the broker holding credentials for all of it. A
// ProbeIdentity verdict can only arise on a first pass, before any topology
// pod exists, so its fence is a no-op riding the same branch. A fence
// that fails does not terminalize: the claim is released and the next pass
// retries the fence. Every other error keeps the launch path's transient
// behaviour — the claim is released back to Pending — because a signal that
// clears on a later pass must not strand the run (terminalPreflightVerdict).
func (r *CoderRunReconciler) secureLaunchFailure(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, err error) (ctrl.Result, error) {
	if permanent := launchRejection(err); permanent != nil {
		if fenceErr := r.Secure.Fence(ctx, run); fenceErr != nil {
			return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, fenceErr)
		}
		return r.Secure.transitionNeedsHuman(ctx, run, permanent.Detail)
	}
	return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
}

// terminateNeedsHuman ends a run whose permanent, actionable deployment
// failure requeueing cannot fix — a missing pull request head, a rendered pod
// spec the API server rejects outright. This is the legacy launcher's ending:
// the claim is released and the run terminalizes NeedsHuman naming the cause,
// rather than cycling claim and release on every reconcile. The secure launch
// path terminalizes the same way through transitionNeedsHuman, which records
// its own SecurePreflightFailed condition and settles the claim through the
// terminal lifecycle report; this path carries the cause in the run's terminal
// diagnostic instead. It leaves the run's branch identity untouched, so a
// fix-pr run keeps the head its terminal PR enrichment observes. The cause is
// handled here, so it is logged and not returned as a reconcile error.
func (r *CoderRunReconciler) terminateNeedsHuman(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, cause *SecureNeedsHumanError) (ctrl.Result, error) {
	log.FromContext(ctx).Info("terminating run: permanent deployment failure a human must fix",
		"run", run.Name, "reason", cause.Reason, "cause", cause.Detail)
	releaseErr := adapter.Release(ctx, item)
	terminalResult, terminalErr := r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "", terminalLifecycleIntent{error: cause.Detail}, nil)
	return terminalResult, errors.Join(releaseErr, terminalErr)
}

// terminateStuckTerminatingCollision bounds the stuck-terminating provisioning
// collision (#560). A run-owned coordinator pod whose deletion is in flight
// holds the deterministic slot; while it stays Terminating (finalizer, delayed
// node eviction) the run requeues as provisioning-in-flight forever. Once the
// pod has been terminating longer than the configured grace, the run
// terminalizes NeedsHuman naming the blocking pod, which frees the lane slot
// (terminal phases do not consume admission capacity) and releases the source
// claim through the terminal lifecycle. The pod itself is never deleted here.
func (r *CoderRunReconciler) terminateStuckTerminatingCollision(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, collision *ProvisioningCollisionError) (ctrl.Result, error) {
	grace := r.provisioningCollisionGrace()
	terminating := r.clock().Sub(collision.Since)
	if terminating < grace {
		// Still inside the grace window: the ordinary 5s
		// provisioning-in-flight requeue continues unchanged.
		return ctrl.Result{RequeueAfter: provisioningInFlightRequeueDelay}, nil
	}
	cause := secureNeedsHuman("ProvisioningCollisionStuckTerminating",
		"coordinator pod %s (uid %s) has been Terminating since %s, beyond the %s provisioning-collision grace; the slot is blocked and a human must clear it",
		collision.PodName, collision.PodUID, collision.Since.UTC().Format(time.RFC3339), grace)
	log.FromContext(ctx).Info("terminating run: coordinator pod stuck Terminating beyond provisioning-collision grace",
		"run", run.Name, "pod", collision.PodName, "podUID", collision.PodUID, "terminating", terminating.String(), "grace", grace.String())
	releaseErr := adapter.Release(ctx, item)
	// The cause Detail alone lives only in the pending LifecycleReported
	// message and can be replaced when the report publishes, so the blocking
	// pod's identity rides its own condition in the same status patch.
	intent := terminalLifecycleIntent{error: cause.Detail}
	intent.conditions = []metav1.Condition{{
		Type:               provisioningCollisionCondition,
		Status:             metav1.ConditionTrue,
		Reason:             provisioningCollisionStuckTerminatingReason,
		Message:            fmt.Sprintf("coordinator pod %s (uid %s) stuck Terminating beyond %s provisioning-collision grace", collision.PodName, collision.PodUID, grace),
		LastTransitionTime: metav1.NewTime(r.clock()),
	}}
	terminalResult, terminalErr := r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "", intent, nil)
	return terminalResult, errors.Join(releaseErr, terminalErr)
}

// resolveCompleted closes the source work for a Done run and then applies the
// reap policy. A valid done-at marker proves Resolve succeeded on an earlier
// reconcile — Courier writes the marker only afterwards — so a run inside its
// retention window never mutates the source again. A missing or malformed
// marker proves nothing: the source resolve must then succeed before the
// retention clock starts, because a run whose source could not be resolved is
// never reaped.
func (r *CoderRunReconciler) resolveCompleted(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	if _, ok := doneAtMarker(run); !ok {
		adapter, item, err := r.adapterAndWorkItem(run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := adapter.Resolve(ctx, item); err != nil {
			return ctrl.Result{}, err
		}
	}
	return r.reapDone(ctx, run)
}

func (r *CoderRunReconciler) observeRunning(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(run.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	// Soft-stop ingress (#238): before any secure observation, translate the
	// human's annotation into the operator-owned control request. The request
	// is fenced to the live coordinator pod, so a secure control and a legacy
	// coordinator are stamped by the same rule; a cleared or changed value is
	// persisted here, before the reconcile continues.
	if err := r.syncSoftStop(ctx, run, pods.Items); err != nil {
		return ctrl.Result{}, err
	}
	// The secure topology's operator-owned observation runs before the
	// coordinator-termination handler below: a terminated control pod must
	// fence its worker and take the broker down in the same reconcile that
	// terminalizes the run. A terminal run is never relaunched, and an
	// AwaitingReview, HandedOff, or BlockedExternal run is never reaped, so fencing left to a
	// later reconcile could never run at all — the untrusted executor and the
	// credentialed broker would outlive their supervisor indefinitely.
	if r.Secure != nil {
		securePods := make([]corev1.Pod, 0, len(pods.Items))
		for i := range pods.Items {
			if podBelongsToRun(&pods.Items[i], run) {
				securePods = append(securePods, pods.Items[i])
			}
		}
		result, handled, err := r.Secure.ObserveTopology(ctx, run, securePods)
		if err != nil || handled {
			return result, err
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podBelongsToRun(pod, run) {
			continue
		}
		// The world, not the operator's clock, knows when the coordinator
		// started: record the container's own start time, once, from whatever
		// state the pod is in. Recording a terminated pod's start before
		// transitionTerminal lets the terminal patch pair a RunDuration with
		// it in the same reconcile.
		if run.Status.StartedAt == nil {
			if start := coordinatorContainerStart(pod); !start.IsZero() {
				before := run.DeepCopy()
				run.Status.StartedAt = &metav1.Time{Time: start}
				if run.Status.WaitDuration == "" {
					run.Status.WaitDuration = durationString(start.Sub(run.CreationTimestamp.Time))
				}
				if err := r.patchStatus(ctx, before, run); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		exitCode, terminationReason, terminationOutcome, terminationSummary, terminated := coordinatorTermination(pod)
		if !terminated {
			continue
		}
		// A coordinator whose deletion is in flight is being deliberately
		// torn down: its recorded exit belongs to that teardown, not to the
		// model's work, and the two are indistinguishable — a wedge-reaped
		// coordinator can carry a cancelled-stream failure exit or even exit
		// 0 inside the shutdown window. Never map it, exactly as
		// topologyBroken, coordinatorPodOf and activeOperationValid treat a
		// terminating pod: checkLiveness re-observes the transition, and the
		// disappearance backstop takes the deferred relaunch charge once the
		// object is provably gone. The mirror case — a meaningful exit
		// followed by deletion of the object (node drain, manual delete) —
		// takes the same pod-loss relaunch by decision: a timestamp ordering
		// test cannot separate the cases, because graceful deletion stamps
		// deletionTimestamp with a value of the request plus the grace
		// period, while the kubelet may end the SIGTERM'd container at any
		// point after the request — a teardown exit can therefore read as
		// finished before the stamp's value. A deleted coordinator is
		// untrusted by construction, and the resumed run re-reads the
		// world. (#369)
		if pod.DeletionTimestamp != nil {
			continue
		}
		// The trusted native terminal result (#127) classifies the ending when
		// one exists for this incarnation: the persisted status result fenced
		// to this pod's UID, or the pod's own validated termination handoff as
		// the fallback channel. The operator classifies from that evidence —
		// never from the exit code or the model's prose alone — and keeps the
		// legacy exit-code mapping for every run without one. An
		// infrastructure failure never terminalizes here: it is the
		// relaunch/resume case bounded by the crashloop counter (HARNESS.md
		// §8), and the world observation that follows a published handoff
		// stays the only route to a successful AwaitingReview.
		trusted, hasTrusted := trustedNativeResult(run, pod)
		if hasTrusted && trusted.Kind == courierv1alpha1.NativeResultInfrastructureFailed {
			return r.relaunchInfrastructureFailure(ctx, run, pod, trusted)
		}
		phase := phaseForExit(run.Spec.Mode, exitCode, terminationOutcome)
		if hasTrusted {
			phase = phaseForNativeResult(run.Spec.Mode, trusted.Kind)
		}
		if phase == courierv1alpha1.PhaseVerifying {
			before := run.DeepCopy()
			// Record a declared no_change_needed fix-pr run (exit 3) so its terminal
			// report can say no change was pushed; exit 0 also lands here but may have
			// pushed a fix, so only the declared exit sets the flag.
			if run.Spec.Mode == courierv1alpha1.ModeFixPR && (exitCode == exitDeclaredNoChange || (hasTrusted && trusted.Kind == courierv1alpha1.NativeResultNoChange)) {
				run.Status.DeclaredNoChange = true
			}
			run.Status.Phase = courierv1alpha1.PhaseVerifying
			if terminationSummary != nil {
				run.Status.Telemetry = terminationSummary
			}
			if err := r.patchStatus(ctx, before, run); err != nil {
				return ctrl.Result{}, err
			}
			r.emitPhaseTransition(run, courierv1alpha1.PhaseVerifying, map[string]any{"exit_code": exitCode})
			return ctrl.Result{Requeue: true}, nil
		}
		// An external-dependency block carries the coordinator's bounded
		// reason into the source report, exactly like a declared question.
		// A no_change_needed fix-pr run no longer terminalizes directly: it
		// is routed to Verifying, which settles it against the world.
		intent := terminalLifecycleIntent{}
		// Workspace-safety evidence is acquired only for the endings whose
		// recovery channel is the fenced capacity path: the confirmed
		// capacity-exhausted handoff, and the exact #455 forge-unavailability
		// pairing (declared blocked_external carried by a validated
		// forge-unavailable handoff category). A task-shaped blocked_external
		// declaration opens nothing here.
		if terminationOutcome == string(executor.OutcomeCapacityExhausted) || (hasTrusted && trusted.Kind == courierv1alpha1.NativeResultCapacityExhausted) || trustedForgeUnavailableHandoff(pod, terminationOutcome) {
			evidenceBefore := run.DeepCopy()
			if r.Secure != nil {
				if message, valid := parseCoordinatorTermination(pod); valid {
					run.Status.WorkspaceSafety = boundedWorkspaceSafety(message.WorkspaceSafety)
				}
			} else {
				safety, pending, err := r.legacyWorkspaceObservation(ctx, run, pod)
				if err != nil {
					return ctrl.Result{}, err
				}
				if pending {
					return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
				}
				run.Status.WorkspaceSafety = safety
				if safety != nil {
					intent.conditions = append(intent.conditions, legacyObservationCondition(run, metav1.NewTime(r.clock())))
				}
			}
			if !reflect.DeepEqual(evidenceBefore.Status.WorkspaceSafety, run.Status.WorkspaceSafety) {
				if err := r.patchStatus(ctx, evidenceBefore, run); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		if phase == courierv1alpha1.PhaseBlockedExternal {
			intent.error = terminationReason
		}
		// A diagnostic explains a non-success ending. Successful handoffs
		// (Verifying, AwaitingReview) carry no failure cause, so they get none.
		// The diagnostic's reason is operator-derived (category + kubelet
		// state); the executor's freeform termination text is intentionally
		// not an input, so a model-controlled needs-decision question or
		// verifier fail-reason cannot land on the durable status.
		if phase == courierv1alpha1.PhaseFailed || phase == courierv1alpha1.PhaseNeedsHuman || phase == courierv1alpha1.PhaseBlockedExternal {
			exit := exitCode
			handoffCategory := coordinatorHandoffCategory(pod)
			handoffValid := coordinatorHandoffValid(pod)
			k8sFailure := kubernetesFailureEvidence(pod, handoffValid)
			trustedKind := courierv1alpha1.NativeResultKind("")
			if hasTrusted {
				trustedKind = trusted.Kind
			}
			intent.diagnostic = buildTerminalDiagnostic(r.clock(), &exit, "", terminationOutcome, handoffCategory, k8sFailure, pod, trustedKind)
		}
		return r.transitionTerminal(ctx, run, phase, "", intent, terminationSummary)
	}
	result, handled, err := r.checkLiveness(ctx, run, pods.Items)
	if err != nil {
		return ctrl.Result{}, err
	}
	if handled {
		return result, nil
	}
	// Liveness reaps a live pod whose activity went silent, and its requeue
	// (the stuck-terminating and deferred-charge cases) is propagated, not
	// dropped. Pod loss needs no heartbeat at all: there is no pod left to
	// heartbeat from, so the backstop is checked independently of the
	// heartbeat's state — legacy pods never write one. The backstop covers
	// both a missing coordinator and one the cache can only see as an
	// unobservable dead object.
	if result.RequeueAfter > 0 || result.Requeue {
		return result, nil
	}
	if coordinator := coordinatorPodOf(pods.Items, run); coordinator == nil || coordinatorPodUnrecoverable(coordinator) {
		return r.observeMissingCoordinator(ctx, run)
	}
	return ctrl.Result{}, nil
}

// maxSoftStopRequestIDLength mirrors the CRD's controlRequest.id maxLength:
// an annotation value beyond it can never be stamped, so ingress skips it
// instead of failing the reconcile.
const maxSoftStopRequestIDLength = 128

// softStopDisarmMessage is the fixed operator message a SoftStopObserved
// condition carries for a request its named incarnation never acknowledged
// (#238), and the record of which annotation value that disarmed request
// carried. A torn-down incarnation's request is void and is never handed to
// its replacement: re-targeting requires a new annotation value, so the same
// value must not stamp again while the observation stands. The message is a
// fixed operator string built only from the opaque request ID, so a
// soft-stop observation can never carry executor or harness text.
func softStopDisarmMessage(id string) string {
	return fmt.Sprintf("soft-stop request %q was never acknowledged by the coordinator before it was torn down; re-targeting requires a new annotation value", id)
}

// softStopDisarmed reports whether the annotation value id has already been
// served or disarmed and must not stamp again: it is the live record, it was
// acknowledged, or it was disarmed unacknowledged by a torn-down
// incarnation. Re-annotating with a fresh value — or withdrawing the
// annotation and re-adding it — is what re-arms a request.
func softStopDisarmed(run *courierv1alpha1.CoderRun, id string) bool {
	if current := run.Status.ControlRequest; current != nil && current.ID == id {
		return true
	}
	if ack := run.Status.ControlRequestAck; ack != nil && ack.ID == id {
		return true
	}
	if cond := apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionSoftStopObserved); cond != nil && cond.Message == softStopDisarmMessage(id) {
		return true
	}
	return false
}

// syncSoftStop translates the human's soft-stop annotation on a Running run
// into the operator-owned control request and persists the change. A value
// never served or disarmed stamps a fresh request fenced to the live
// coordinator pod and clears any unacknowledged-observation; a cleared
// annotation clears the record, its acknowledgement, and its observation;
// a value already served, acknowledged, or disarmed — or a vanished
// coordinator — leaves the record untouched. It is the sole writer of
// status.controlRequest and never charges or relaunches the run.
func (r *CoderRunReconciler) syncSoftStop(ctx context.Context, run *courierv1alpha1.CoderRun, pods []corev1.Pod) error {
	id := run.Annotations[courierv1alpha1.SoftStopAnnotation]
	if len(id) > maxSoftStopRequestIDLength {
		// The request ID is CRD-bounded; a longer annotation cannot become a
		// record. Skip the stamp rather than failing the reconcile: an
		// invalid human value must not wedge observation or liveness for the
		// run. Correcting the annotation re-arms ingress.
		return nil
	}
	if id == "" {
		if run.Status.ControlRequest == nil && run.Status.ControlRequestAck == nil &&
			apimeta.FindStatusCondition(run.Status.Conditions, courierv1alpha1.ConditionSoftStopObserved) == nil {
			return nil
		}
		before := run.DeepCopy()
		// The human withdrew the soft-stop: clear the record, its
		// acknowledgement, and its unacknowledged-observation, so a later
		// value — even a repeat of an old one — is a fresh request.
		run.Status.ControlRequest = nil
		run.Status.ControlRequestAck = nil
		clearSoftStopCondition(run)
		return r.patchStatus(ctx, before, run)
	}
	if softStopDisarmed(run, id) {
		return nil
	}
	pod := coordinatorPodOf(pods, run)
	if pod == nil {
		// A request is fenced to one control incarnation; with no live
		// coordinator there is nothing to address it to, so it is not
		// stamped.
		return nil
	}
	before := run.DeepCopy()
	// A new value stamps a fresh request and clears any prior
	// unacknowledged-observation.
	run.Status.ControlRequest = &courierv1alpha1.ControlRequest{
		ID:           id,
		Kind:         courierv1alpha1.ControlRequestSoftStop,
		TargetPodUID: string(pod.UID),
		RequestedAt:  metav1.NewTime(r.clock()),
	}
	clearSoftStopCondition(run)
	return r.patchStatus(ctx, before, run)
}

// clearSoftStopCondition removes the SoftStopObserved condition from a run.
func clearSoftStopCondition(run *courierv1alpha1.CoderRun) {
	apimeta.RemoveStatusCondition(&run.Status.Conditions, courierv1alpha1.ConditionSoftStopObserved)
}

// softStopBeforeRelaunch disarms a run's control request before its control
// incarnation is torn down by a relaunch. A request addressed to the dying
// incarnation is void: the record is cleared so a fresh incarnation starts
// from a clean control state, and the value that was disarmed is remembered
// so the replacement is never handed its predecessor's request. If the named
// incarnation never acknowledged the request, the run records that as a
// SoftStopObserved observation; an acknowledged request is cleared silently,
// its acknowledgement retained as the record of the settled value.
func softStopBeforeRelaunch(run *courierv1alpha1.CoderRun, now time.Time) {
	record := run.Status.ControlRequest
	if record == nil {
		return
	}
	if ack := run.Status.ControlRequestAck; ack == nil || ack.ID != record.ID {
		apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type:               courierv1alpha1.ConditionSoftStopObserved,
			Status:             metav1.ConditionTrue,
			Reason:             courierv1alpha1.ConditionReasonUnacknowledged,
			Message:            softStopDisarmMessage(record.ID),
			LastTransitionTime: metav1.NewTime(now),
		})
	}
	// Only the request is disarmed: an acknowledgement (matching or older)
	// stays as the settled-value memory that keeps a repeated annotation
	// from re-targeting the replacement.
	run.Status.ControlRequest = nil
}

// controllerHeadRef reconstructs the run's resolved head identity from its
// status. An empty status.headRepo means a same-repository head: the observer
// treats the base repository as the head repository, which also preserves the
// behavior of runs persisted before head identity was recorded.
func controllerHeadRef(run *courierv1alpha1.CoderRun) HeadRef {
	return HeadRef{Repo: run.Status.HeadRepo, Branch: run.Status.Branch, SHA: run.Status.HeadSHA}
}

func (r *CoderRunReconciler) observeVerifying(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	if r.Observer == nil {
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "", terminalLifecycleIntent{}, nil)
	}
	observation, err := r.Observer.Observe(ctx, run.Spec.Repo, controllerHeadRef(run))
	if err != nil {
		return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
	}
	pr := observation.PR
	state := observeState(observation)
	if state == observationMerged {
		// Someone merged the PR while this run was working: the work shipped.
		// Done resolves the source and applies the reap policy; it is not a
		// case for a human.
		//
		// The merged branch is checked before the park loop runs, so a parked
		// run reaches Done here with its park record and its deferral intact
		// — exactly the shape every loop bail-out hands to the abandon. The
		// deferral must be settled the same way before the run terminalizes:
		// the abandon is a no-op without a park record and a live fence, and
		// it settles the reservation under a live one rather than leaving it
		// stranded against a run that is now Done (#555).
		if _, err := r.abandonCIPark(ctx, run, observation); err != nil {
			return ctrl.Result{}, err
		}
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseDone, pr, terminalLifecycleIntent{}, nil)
	}
	// The mechanical-CI park-and-resume policy, when the lane declares one,
	// owns every observation that is not already terminal. It reports
	// handled=false when no policy applies, and the ordinary grading path
	// below is then unchanged.
	handled, result, err := r.observeCIPark(ctx, run, observation)
	if err != nil {
		return ctrl.Result{}, err
	}
	if handled {
		return result, nil
	}
	if state == observationFailed {
		// Queue-backed fix-pr work has a source-owned retry policy. Keep
		// resolve-issue observers alive so this run can carry the original issue
		// to review after the PR is repaired by a later run.
		if run.Spec.Mode != courierv1alpha1.ModeFixPR {
			fingerprint := checkSetFingerprint(observation)
			settled := fingerprint != "" && fingerprint == run.Status.FailedCheckFingerprint
			before := run.DeepCopy()
			if pr != "" {
				run.Status.PR = pr
			}
			run.Status.CheckFingerprint = ""
			run.Status.FailedCheckFingerprint = fingerprint
			if err := r.patchStatus(ctx, before, run); err != nil {
				return ctrl.Result{}, err
			}
			if settled {
				return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseHandedOff, pr, terminalLifecycleIntent{}, nil)
			}
			return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
		}
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseFailed, pr, terminalLifecycleIntent{ciFailure: true}, nil)
	}
	if state == observationNeedsHuman {
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, pr, terminalLifecycleIntent{}, nil)
	}
	// status.checkFingerprint is the prior all-green candidate: the identity
	// of the last poll on which every observed check passed. Green settles
	// only onto an identical candidate — two consecutive all-green
	// observations of the same check set. A pending or empty observation
	// clears the candidate, because checks that have not registered yet can
	// still appear at any later poll; a changed identity resets it the same
	// way. A fix-pr red observation is recognized immediately and skips the
	// green settle; a resolve-issue red observation waits for a second
	// consecutive identical fully-failed observation before handing off.
	fingerprint := ""
	if state == observationPassed {
		fingerprint = checkSetFingerprint(observation)
	}
	settled := fingerprint != "" && fingerprint == run.Status.CheckFingerprint
	before := run.DeepCopy()
	if pr != "" {
		run.Status.PR = pr
	}
	run.Status.CheckFingerprint = fingerprint
	run.Status.FailedCheckFingerprint = ""
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, err
	}
	if settled {
		// Green CI does not prove queued review feedback was addressed. A
		// no-change attempt on an open PR waits for authoritative resolution
		// instead of claiming a push or authorizing already-addressed settlement.
		if run.Status.DeclaredNoChange && run.Spec.Mode == courierv1alpha1.ModeFixPR {
			return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseBlockedExternal, pr, terminalLifecycleIntent{
				error:      "no changes pushed; queued feedback resolution is not verified",
				diagnostic: buildTerminalDiagnostic(r.clock(), nil, "", "", "", false, nil, ""),
			}, nil)
		}
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseAwaitingReview, pr, terminalLifecycleIntent{}, nil)
	}
	return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
}

// enrichTerminalPR best-effort refreshes the observed PR for a run that
// reaches a NeedsHuman or Failed terminal state without passing through
// Verifying, so a PR the coordinator opened is still published. It never
// changes the run's phase and never blocks terminalization: with no observer,
// no resolved branch, or an observation error it returns the run's
// best-known PR, preserving any already-recorded value.
func (r *CoderRunReconciler) enrichTerminalPR(ctx context.Context, run *courierv1alpha1.CoderRun) string {
	pr := run.Status.PR
	if r.Observer == nil || run.Status.Branch == "" {
		return pr
	}
	observation, err := r.Observer.Observe(ctx, run.Spec.Repo, controllerHeadRef(run))
	if err != nil {
		log.FromContext(ctx).V(1).Info("terminal PR enrichment skipped; observation failed",
			"repo", run.Spec.Repo, "branch", run.Status.Branch, "pr", pr, "error", err.Error())
		return pr
	}
	if observation.PR == "" {
		return pr
	}
	return observation.PR
}

func (r *CoderRunReconciler) transitionTerminal(ctx context.Context, run *courierv1alpha1.CoderRun, phase courierv1alpha1.Phase, pr string, intent terminalLifecycleIntent, telemetry *courierv1alpha1.RunTelemetry) (ctrl.Result, error) {
	if phase == courierv1alpha1.PhaseNeedsHuman || phase == courierv1alpha1.PhaseFailed || phase == courierv1alpha1.PhaseBlockedExternal {
		if pr == "" {
			pr = r.enrichTerminalPR(ctx, run)
		}
	}
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	phaseChanged := run.Status.Phase != phase
	// FinishedAt is set once, on the run's first terminal transition, so its
	// absence marks this reconcile as the one that records the run's metrics.
	firstTerminal := run.Status.FinishedAt == nil
	before := run.DeepCopy()
	// The run's control request is void once it is torn down: clear it and its
	// acknowledgement so the terminal state carries no live request (#238).
	run.Status.ControlRequest = nil
	run.Status.ControlRequestAck = nil
	// A repair attempt still in flight is abandoned, not left dangling: its
	// fence is settled as unsupported on this same write so the source stops
	// deferring against a run that can never report (#547).
	abandonCIRepairAttempt(run)
	run.Status.Phase = phase
	if pr != "" {
		run.Status.PR = pr
	}
	if telemetry != nil {
		run.Status.Telemetry = telemetry
	}
	if phaseChanged {
		// finishedAt marks when the run's own execution ended: it is
		// recorded on every terminal transition, set-once, so a later
		// merge-confirmed Done (#250) never moves it.
		if run.Status.FinishedAt == nil {
			run.Status.FinishedAt = &metav1.Time{Time: r.clock().UTC()}
			if run.Status.StartedAt != nil && run.Status.RunDuration == "" {
				run.Status.RunDuration = durationString(run.Status.FinishedAt.Time.Sub(run.Status.StartedAt.Time))
			}
		}
		if intent.diagnostic != nil && run.Status.TerminalDiagnostic == nil {
			run.Status.TerminalDiagnostic = intent.diagnostic
		}
		// Caller-supplied conditions land in the same status patch as the phase
		// change, so the recorded reason for the transition is never split from
		// it across a failed write.
		for _, cond := range intent.conditions {
			apimeta.SetStatusCondition(&run.Status.Conditions, cond)
		}
		// Persist report intent with the phase so retries can reproduce the same
		// lifecycle without extending the CRD status schema.
		r.setLifecycleReport(run, false, pendingLifecycleReason(intent), intent.error)
	}
	// Write the terminal phase first so the run stops holding lane capacity
	// regardless of whether the source lifecycle report below succeeds. The
	// run's own terminalization must never depend on the source.
	if run.Status.Phase != before.Status.Phase || run.Status.PR != before.Status.PR || !reflect.DeepEqual(run.Status.Telemetry, before.Status.Telemetry) || !reflect.DeepEqual(run.Status.TerminalDiagnostic, before.Status.TerminalDiagnostic) || !reflect.DeepEqual(run.Status.ControlRequest, before.Status.ControlRequest) || !reflect.DeepEqual(run.Status.ControlRequestAck, before.Status.ControlRequestAck) || !reflect.DeepEqual(run.Status.WorkspaceSafety, before.Status.WorkspaceSafety) || !reflect.DeepEqual(run.Status.Conditions, before.Status.Conditions) || !reflect.DeepEqual(run.Status.CIRepair, before.Status.CIRepair) || !reflect.DeepEqual(run.Status.CISettlement, before.Status.CISettlement) {
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if run.Status.Phase != before.Status.Phase {
		r.emitPhaseTransition(run, phase, map[string]any{"branch": run.Status.Branch, "pr": run.Status.PR})
	}
	if !phaseChanged {
		// Already in this terminal phase; the lifecycle was published (or
		// dropped) on the reconcile that entered it.
		return ctrl.Result{}, nil
	}
	// The run's metrics are recorded once, on its first terminal transition.
	// A later transition that corrects the ending — a confirmed merge
	// settling AwaitingReview as Done (#250) — must not count the same run
	// twice.
	if firstTerminal {
		r.metrics().ObserveTerminal(run)
		r.metrics().ObserveTerminalDiagnostic(run)
	}
	return r.publishTerminalLifecycle(ctx, run, adapter, item, phase, intent)
}

// pendingLifecycleReason stores retry intent in the standard condition reason;
// its message preserves a coordinator explanation across report retries.
func pendingLifecycleReason(intent terminalLifecycleIntent) string {
	if intent.ciFailure {
		return lifecycleReportPendingExternalVerificationFailureReason
	}
	if intent.skipPRFixQueueMark {
		return lifecycleReportPendingBlockedReportParksPRFixReason
	}
	if intent.error != "" {
		return lifecycleReportPendingWithErrorReason
	}
	return lifecycleReportPendingReason
}

func pendingLifecycleIntent(run *courierv1alpha1.CoderRun) terminalLifecycleIntent {
	cond := apimeta.FindStatusCondition(run.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		return terminalLifecycleIntent{}
	}
	return terminalLifecycleIntent{
		skipPRFixQueueMark: cond.Reason == lifecycleReportPendingBlockedReportParksPRFixReason,
		ciFailure:          cond.Reason == lifecycleReportPendingExternalVerificationFailureReason,
		error:              cond.Message,
	}
}

// publishTerminalLifecycle reports the terminal lifecycle to the source and
// records the outcome on the run's LifecycleReported condition. The run is
// already in its terminal phase, so no outcome here blocks terminalization.
// A superseded rejection means this attempt was replaced by a newer generation
// of the same work: the report is dropped, not retried. A permanent refusal
// means the source rejected the report for a durable reason: the report
// settles as refused, not retried. A transient failure leaves a pending
// report that a later reconcile retries; the source's idempotency key makes
// the retry safe. The run itself is never held by the report.
func (r *CoderRunReconciler) publishTerminalLifecycle(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, phase courierv1alpha1.Phase, intent terminalLifecycleIntent) (ctrl.Result, error) {
	err := r.publishSourceLifecycle(ctx, run, adapter, item, phase, intent)
	retryToken := run.Status.LifecycleReportActiveRetryToken
	if retryToken == "" && lifecycleReportUnsupported(run) && lifecycleReportRetryRequested(run) {
		retryToken = run.Annotations[retryLifecycleReportAnnotation]
	}
	switch {
	case err == nil:
		if markErr := r.markLifecycleReportOutcome(ctx, run, true, lifecycleReportPublishedReason, "", retryToken, lifecycleReportAcceptedSupportReason); markErr != nil {
			return ctrl.Result{}, markErr
		}
		// A capacity-exhaustion ending is not finished with the source until
		// the attempt is settled with fencing: the published report makes the
		// work recoverable, and the fenced settlement releases this attempt's
		// ownership so a compatible worker can take it (#327).
		if isCapacityExhaustionRun(run) {
			return r.recoverCapacityExhaustion(ctx, run, adapter, item)
		}
		return ctrl.Result{}, nil
	case errors.Is(err, source.ErrSuperseded):
		log.FromContext(ctx).Info("dropping superseded source lifecycle report",
			"run", run.Name, "phase", phase, "error", err.Error())
		return ctrl.Result{}, r.markLifecycleReportOutcome(ctx, run, true, lifecycleReportSupersededReason, err.Error(), "", lifecycleReportSupersededReason)
	case errors.Is(err, source.ErrUnsupportedOutcome):
		log.FromContext(ctx).Info("source does not support terminal lifecycle outcome; awaiting explicit retry",
			"run", run.Name, "phase", phase, "error", err.Error())
		return ctrl.Result{}, r.markLifecycleReportOutcome(ctx, run, false, pendingLifecycleReason(intent), intent.error, retryToken, lifecycleReportUnsupportedOutcomeReason)
	case errors.Is(err, source.ErrPermanentRefusal):
		// The source refused the report for a durable reason — a closed
		// target or a worker identity that no longer owns the work — and
		// retrying the same report can never change that. Settle it so the
		// run's retention clock starts instead of retrying every
		// lifecycleReportRequeueDelay forever (#561).
		log.FromContext(ctx).Info("settling terminal lifecycle report permanently refused by the source",
			"run", run.Name, "phase", phase, "error", err.Error())
		return ctrl.Result{}, r.markLifecycleReportOutcome(ctx, run, true, lifecycleReportPermanentRefusalReason, err.Error(), "", lifecycleReportPermanentRefusalReason)
	default:
		log.FromContext(ctx).Info("terminal lifecycle report failed; will retry",
			"run", run.Name, "phase", phase, "error", err.Error())
		// Keep the explanation in the pending condition so a declared block's
		// lifecycle report can be reproduced exactly on every retry.
		retryToken := run.Status.LifecycleReportActiveRetryToken
		if retryToken == "" && lifecycleReportUnsupported(run) && lifecycleReportRetryRequested(run) {
			retryToken = run.Annotations[retryLifecycleReportAnnotation]
		}
		return ctrl.Result{RequeueAfter: lifecycleReportRequeueDelay}, r.markLifecycleReportOutcome(ctx, run, false, pendingLifecycleReason(intent), intent.error, retryToken, lifecycleReportRetryableReason)
	}
}

func (r *CoderRunReconciler) publishSourceLifecycle(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, phase courierv1alpha1.Phase, intent terminalLifecycleIntent) error {
	state := stateForTerminalPhase(phase, run.Spec.Mode, intent)
	legacyNoChange := phase == courierv1alpha1.PhaseAwaitingReview && run.Status.DeclaredNoChange && run.Spec.Mode == courierv1alpha1.ModeFixPR
	if legacyNoChange {
		state = source.StateInProgress
	}
	if isCapacityExhaustionRun(run) {
		lifecycle := lifecycleForPhase(phase, state, run.Status.PR, startedAtForRun(run), intent)
		lifecycle.Result = source.ResultCapacityExhausted
		lifecycle.IdempotencyKey = lifecycleIdempotencyKey(run, phase)
		return reportLifecycle(ctx, adapter, item, lifecycle)
	}
	if state != "" {
		if err := adapter.Transition(ctx, item, state); err != nil {
			return err
		}
	}
	var startedAt *time.Time
	if run.Status.StartedAt != nil {
		// Copy the time to a local before taking its address: the lifecycle
		// report must carry the observed start, not a pointer into the run's
		// live status that a later mutation could move.
		t := run.Status.StartedAt.Time
		startedAt = &t
	}
	lifecycle := lifecycleForPhase(phase, state, run.Status.PR, startedAt, intent)
	// Fail closed for retained AwaitingReview runs written before the
	// no-change verification gate: replay must not certify unresolved feedback.
	if legacyNoChange {
		lifecycle.Result = source.ResultBlockedExternal
		lifecycle.Error = "no changes pushed; queued feedback resolution is not verified"
	}
	lifecycle.IdempotencyKey = lifecycleIdempotencyKey(run, phase)
	if run.Status.Telemetry != nil {
		if raw, err := json.Marshal(run.Status.Telemetry); err == nil {
			lifecycle.Telemetry = raw
		}
	}
	return reportLifecycle(ctx, adapter, item, lifecycle)
}

// stateForTerminalPhase is the source state published for a terminal phase.
// Only phases reached through transitionTerminal carry a state; everything else
// publishes none.
func stateForTerminalPhase(phase courierv1alpha1.Phase, runMode courierv1alpha1.Mode, intent terminalLifecycleIntent) source.State {
	// Only a fix-pr external-check failure is retryable by its source. Other
	// terminal failures are parked for human attention rather than left active.
	switch phase {
	case courierv1alpha1.PhaseAwaitingReview:
		return source.StateInReview
	case courierv1alpha1.PhaseBlockedExternal:
		// A recoverable block keeps the source item active so it can be
		// re-issued when the prerequisite is satisfied; it is not parked
		// for a human.
		return source.StateInProgress
	case courierv1alpha1.PhaseNeedsHuman:
		return source.StateNeedsHuman
	case courierv1alpha1.PhaseFailed:
		if runMode == courierv1alpha1.ModeFixPR && intent.ciFailure {
			return source.StateInProgress
		}
		return source.StateNeedsHuman
	default:
		return ""
	}
}

// setLifecycleReport records the terminal lifecycle-report condition on the run
// in memory; callers persist it through patchStatus. The operator is the only
// writer of a run's conditions.
func (r *CoderRunReconciler) setLifecycleReport(run *courierv1alpha1.CoderRun, published bool, reason, message string) {
	statusValue := metav1.ConditionTrue
	if !published {
		statusValue = metav1.ConditionFalse
	}
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               lifecycleReportedCondition,
		Status:             statusValue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.clock()),
	})
}

func (r *CoderRunReconciler) markLifecycleReportOutcome(ctx context.Context, run *courierv1alpha1.CoderRun, published bool, reason, message, retryToken, supportReason string) error {
	before := run.DeepCopy()
	if reason != lifecycleReportUnsupportedOutcomeReason {
		r.setLifecycleReport(run, published, reason, message)
	}
	switch supportReason {
	case lifecycleReportUnsupportedOutcomeReason:
		r.setLifecycleReportCapability(run, metav1.ConditionFalse, lifecycleReportUnsupportedOutcomeReason, "source rejected this lifecycle outcome as unsupported")
	case lifecycleReportAcceptedSupportReason:
		r.setLifecycleReportCapability(run, metav1.ConditionTrue, lifecycleReportAcceptedSupportReason, "source accepted this lifecycle outcome")
	case lifecycleReportSupersededReason:
		r.setLifecycleReportCapability(run, metav1.ConditionUnknown, lifecycleReportSupersededReason, "source lifecycle report was superseded before acceptance could be determined")
	case lifecycleReportPermanentRefusalReason:
		r.setLifecycleReportCapability(run, metav1.ConditionUnknown, lifecycleReportPermanentRefusalReason, "source permanently refused this lifecycle report; retrying the same report cannot succeed")
	case lifecycleReportRetryableReason:
		r.setLifecycleReportCapability(run, metav1.ConditionUnknown, lifecycleReportRetryableReason, "source report failed transiently; automatic retry remains enabled")
	}
	if supportReason == lifecycleReportRetryableReason {
		if retryToken != "" {
			run.Status.LifecycleReportActiveRetryToken = retryToken
		}
	} else {
		if retryToken != "" {
			run.Status.LifecycleReportRetryToken = retryToken
		}
		run.Status.LifecycleReportActiveRetryToken = ""
	}
	err := r.patchStatus(ctx, before, run)
	if err == nil {
		// Every call to markLifecycleReportOutcome resolves exactly one
		// report attempt — the first publication at terminalization or one
		// retry via retryTerminalLifecycle — so one attempt outcome is
		// counted per call. A settled report never re-enters here: the
		// reconcile gates skip a run whose LifecycleReported is positively
		// settled, so re-reconciles never double-count. Pending outcomes are
		// not results; the unsettled backlog is observable through
		// courier_coderuns_lifecycle_pending.
		r.recordLifecycleReportOutcome(run.Spec.Source, run.Spec.Lane, run.Spec.Mode, reason, supportReason)
	}
	return err
}

// recordLifecycleReportOutcome maps one report attempt's outcome arguments to
// one of the five bounded report outcomes: Published, Superseded, and
// PermanentRefusal are terminal report states, while UnsupportedOutcome and
// Retryable arrive on the capability condition.
func (r *CoderRunReconciler) recordLifecycleReportOutcome(sourceName, lane string, mode courierv1alpha1.Mode, reason, supportReason string) {
	rec := r.metrics()
	switch {
	case supportReason == lifecycleReportUnsupportedOutcomeReason:
		rec.ObserveLifecycleReportOutcome(sourceName, lane, mode, lifecycleOutcomeUnsupported)
	case supportReason == lifecycleReportRetryableReason:
		rec.ObserveLifecycleReportOutcome(sourceName, lane, mode, lifecycleOutcomeRetryable)
	case reason == lifecycleReportPublishedReason:
		rec.ObserveLifecycleReportOutcome(sourceName, lane, mode, lifecycleOutcomePublished)
	case reason == lifecycleReportSupersededReason:
		rec.ObserveLifecycleReportOutcome(sourceName, lane, mode, lifecycleOutcomeSuperseded)
	case reason == lifecycleReportPermanentRefusalReason:
		rec.ObserveLifecycleReportOutcome(sourceName, lane, mode, lifecycleOutcomePermanentRefusal)
	}
}

func (r *CoderRunReconciler) setLifecycleReportCapability(run *courierv1alpha1.CoderRun, statusValue metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: lifecycleReportSupportedCondition, Status: statusValue, Reason: reason, Message: message,
		LastTransitionTime: metav1.NewTime(r.clock()),
	})
}

// lifecycleReportPending reports a terminal run whose lifecycle report has not
// yet been published. Compatibility is tracked independently so this condition
// retains the original report intent for retries.
func lifecycleReportPending(run *courierv1alpha1.CoderRun) bool {
	switch run.Status.Phase {
	case courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseHandedOff, courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseBlockedExternal:
	default:
		return false
	}
	cond := apimeta.FindStatusCondition(run.Status.Conditions, lifecycleReportedCondition)
	return cond != nil && cond.Status == metav1.ConditionFalse
}

// lifecycleReportSettled reports whether the run's source lifecycle report has
// reached the positive, settled state DESIGN.md §1291 demands before world
// observation can close the loop: the LifecycleReported condition carries
// Status=True with reason Published, Superseded, or PermanentRefusal. A
// missing, Unknown, or False condition does not satisfy the gate, and
// Status=True with any other reason is treated as not yet settled too — only
// these are the reasons the condition's Status=True carries in practice, but
// the helper defends the invariant by name. The gate keeps a legacy,
// partially upgraded, or corrupted terminal CoderRun from being finished by
// world observation before the source has proof of the in-review lifecycle.
func lifecycleReportSettled(run *courierv1alpha1.CoderRun) bool {
	cond := apimeta.FindStatusCondition(run.Status.Conditions, lifecycleReportedCondition)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return false
	}
	return cond.Reason == lifecycleReportPublishedReason || cond.Reason == lifecycleReportSupersededReason || cond.Reason == lifecycleReportPermanentRefusalReason
}

// lifecycleReportConditionState renders the LifecycleReported condition as a
// short, log-friendly string: "<missing>" when absent, otherwise
// "<status>/<reason>". Used only for diagnostic logging.
func lifecycleReportConditionState(run *courierv1alpha1.CoderRun) string {
	cond := apimeta.FindStatusCondition(run.Status.Conditions, lifecycleReportedCondition)
	if cond == nil {
		return "<missing>"
	}
	return string(cond.Status) + "/" + cond.Reason
}

func lifecycleReportUnsupported(run *courierv1alpha1.CoderRun) bool {
	cond := apimeta.FindStatusCondition(run.Status.Conditions, lifecycleReportSupportedCondition)
	return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == lifecycleReportUnsupportedOutcomeReason
}

func lifecycleReportRetryRequested(run *courierv1alpha1.CoderRun) bool {
	token, ok := run.Annotations[retryLifecycleReportAnnotation]
	return ok && validLifecycleReportRetryToken(token) && token != run.Status.LifecycleReportRetryToken
}

func lifecycleReportRetryTokenInvalid(run *courierv1alpha1.CoderRun) bool {
	token, ok := run.Annotations[retryLifecycleReportAnnotation]
	return ok && !validLifecycleReportRetryToken(token)
}

func validLifecycleReportRetryToken(token string) bool {
	return token != "" && utf8.ValidString(token) && utf8.RuneCountInString(token) <= 128
}

// reconcileEvidence owns the run's failure-evidence lifecycle at the terminal
// boundary the operator can prove. A run the operator has world-verified as
// landed (AwaitingReview, Done) has its evidence Secrets deleted; a dirty
// terminal (Failed, NeedsHuman) run gains the informational EvidenceCaptured
// condition when its evidence is present in the world. It never touches the
// phase: the intake is the only writer of the evidence Secrets and the
// condition is derived from the world, so a nil intake or a phase with no
// evidence action is a no-op, and a transient API failure requeues.
func (r *CoderRunReconciler) reconcileEvidence(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	if r.Evidence == nil {
		return nil
	}
	switch run.Status.Phase {
	case courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseDone:
		// The operator's own world observation proved the work landed, so the
		// evidence it captured is no longer needed. The delete is idempotent: a
		// run with no Secret simply lists nothing and leaves nothing to delete.
		return r.Evidence.DeleteEvidenceForRun(ctx, run)
	case courierv1alpha1.PhaseFailed, courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseBlockedExternal:
		secrets, err := r.Evidence.ListEvidenceForRun(ctx, run)
		if err != nil {
			return err
		}
		if len(secrets) == 0 {
			// No Secret in the world. The condition is informational, and
			// absence of a Secret is never proof of absence, so neither set nor
			// remove it: a terminal run that already captured keeps its mark.
			return nil
		}
		before := run.DeepCopy()
		if !r.setEvidenceCaptured(run) {
			// The condition already carries this exact state; a terminal run
			// must not emit a redundant status patch on every reconcile.
			return nil
		}
		return r.patchStatus(ctx, before, run)
	}
	return nil
}

// setEvidenceCaptured records the informational EvidenceCaptured condition on
// the run in memory; the caller persists it through patchStatus. It reports
// whether the condition changed, so an unchanged terminal run skips the write.
func (r *CoderRunReconciler) setEvidenceCaptured(run *courierv1alpha1.CoderRun) bool {
	return apimeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               EvidenceConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             EvidenceReasonCaptured,
		Message:            fmt.Sprintf("failure evidence Secrets exist; retrieve by label %s=%s", EvidenceLabelKey, evidenceLabelValue(run.Name)),
		ObservedGeneration: run.Generation,
		LastTransitionTime: metav1.NewTime(r.clock()),
	})
}

// retryTerminalLifecycle re-attempts a terminal run's pending source report on
// a later reconcile. The phase is already durable, so this only ever touches the
// report and its condition.
func (r *CoderRunReconciler) retryTerminalLifecycle(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.publishTerminalLifecycle(ctx, run, adapter, item, run.Status.Phase, pendingLifecycleIntent(run))
}

// lifecycleIdempotencyKey derives the publication identity from the run's
// namespace, name, and the terminal phase. It is deterministic across
// reconciles, so a retry after a failed status patch re-reports the same
// lifecycle with the same key and a deduplicating source ignores it.
func lifecycleIdempotencyKey(run *courierv1alpha1.CoderRun, phase courierv1alpha1.Phase) string {
	key := fmt.Sprintf("coderun/%s/%s/%s", run.Namespace, run.Name, string(phase))
	if run.UID != "" {
		key = key + "/" + string(run.UID)
	}
	return key
}

func reportLifecycle(ctx context.Context, adapter source.Adapter, item source.WorkItem, lifecycle source.Lifecycle) error {
	reporter, ok := adapter.(source.Reporter)
	if !ok {
		return nil
	}
	return reporter.Report(ctx, item, lifecycle)
}

// lifecycleForPhase maps a terminal phase to its source lifecycle report.
// startedAt is the recorded coordinator start, so sources can report real run
// durations; nil when the run never reached a recorded start.
func startedAtForRun(run *courierv1alpha1.CoderRun) *time.Time {
	if run.Status.StartedAt == nil {
		return nil
	}
	t := run.Status.StartedAt.Time
	return &t
}

func lifecycleForPhase(phase courierv1alpha1.Phase, state source.State, pr string, startedAt *time.Time, intent terminalLifecycleIntent) source.Lifecycle {
	lifecycle := source.Lifecycle{State: state, PR: pr, StartedAt: startedAt}
	switch phase {
	case courierv1alpha1.PhaseAwaitingReview:
		if pr != "" {
			lifecycle.Result = source.ResultReady
		}
	case courierv1alpha1.PhaseFailed:
		if intent.ciFailure {
			lifecycle.Result = source.ResultFailed
			lifecycle.Error = externalVerificationFailureReason
		} else {
			// Coordinator failures are not an external CI attempt. Park the
			// source for human attention rather than triggering queue retries.
			lifecycle.Result = source.ResultBlocked
			lifecycle.Error = "coordinator failed"
		}
	case courierv1alpha1.PhaseHandedOff:
		lifecycle.Result = source.ResultHandedOff
		lifecycle.Error = externalVerificationFailureReason
	case courierv1alpha1.PhaseNeedsHuman:
		lifecycle.Result = source.ResultBlocked
		lifecycle.Error = intent.error
		if lifecycle.Error == "" {
			lifecycle.Error = "run requires human intervention"
		}
		lifecycle.BlockedReportParksPRFix = intent.skipPRFixQueueMark
	case courierv1alpha1.PhaseBlockedExternal:
		lifecycle.Result = source.ResultBlockedExternal
		lifecycle.Error = intent.error
		if lifecycle.Error == "" {
			lifecycle.Error = "run is blocked on an external prerequisite"
		}
	}
	return lifecycle
}

// patchStatus writes a narrow JSON merge patch through the status subresource.
// It includes only changed operator-owned fields, preserving harness-owned
// fields that the patch omits.
func (r *CoderRunReconciler) patchStatus(ctx context.Context, before, after *courierv1alpha1.CoderRun) error {
	if before == nil || after == nil {
		return errors.New("coderun controller: status patch requires a run")
	}
	// PhaseEnteredAt is replaceable, not set-once: every phase change
	// restamps it with the reconciler clock's now in the same write, so the
	// persisted marker always matches the persisted phase.
	now := r.clock().UTC()
	phaseChanged := before.Status.Phase != after.Status.Phase
	if phaseChanged {
		after.Status.PhaseEnteredAt = &metav1.Time{Time: now}
	}
	var fields status.OperatorPatch
	if before.Status.Phase != after.Status.Phase {
		fields.Phase = after.Status.Phase
	}
	if before.Status.PhaseEnteredAt != after.Status.PhaseEnteredAt {
		fields.PhaseEnteredAt = after.Status.PhaseEnteredAt
	}
	if before.Status.Branch != after.Status.Branch {
		branch := after.Status.Branch
		fields.Branch = &branch
	}
	if before.Status.HeadRepo != after.Status.HeadRepo {
		headRepo := after.Status.HeadRepo
		fields.HeadRepo = &headRepo
	}
	if before.Status.HeadSHA != after.Status.HeadSHA {
		headSHA := after.Status.HeadSHA
		fields.HeadSHA = &headSHA
	}
	if before.Status.PR != after.Status.PR {
		fields.PR = after.Status.PR
	}
	if !reflect.DeepEqual(before.Status.WorkspaceSafety, after.Status.WorkspaceSafety) {
		fields.WorkspaceSafety = after.Status.WorkspaceSafety
	}
	// SourceReceipt is replaceable, not set-once: a re-claim around admission
	// rotates the source fencing token, and settlement must fence against the
	// live claim, so a changed non-empty receipt is re-emitted (#388). An
	// empty value alone never clears a persisted receipt.
	if before.Status.SourceReceipt != after.Status.SourceReceipt && after.Status.SourceReceipt != "" {
		receipt := after.Status.SourceReceipt
		fields.SourceReceipt = &receipt
	}
	// Set-once fields: emitted only on the empty-to-set transition, so a
	// patch can never clear a recorded timestamp.
	if before.Status.AdmittedAt == nil && after.Status.AdmittedAt != nil {
		fields.AdmittedAt = after.Status.AdmittedAt
	}
	if before.Status.StartedAt == nil && after.Status.StartedAt != nil {
		fields.StartedAt = after.Status.StartedAt
	}
	if before.Status.FinishedAt == nil && after.Status.FinishedAt != nil {
		fields.FinishedAt = after.Status.FinishedAt
	}
	if before.Status.WaitDuration == "" && after.Status.WaitDuration != "" {
		fields.WaitDuration = after.Status.WaitDuration
	}
	if before.Status.RunDuration == "" && after.Status.RunDuration != "" {
		fields.RunDuration = after.Status.RunDuration
	}
	if before.Status.CheckFingerprint != after.Status.CheckFingerprint {
		fingerprint := after.Status.CheckFingerprint
		fields.CheckFingerprint = &fingerprint
	}
	if before.Status.FailedCheckFingerprint != after.Status.FailedCheckFingerprint {
		fingerprint := after.Status.FailedCheckFingerprint
		fields.FailedCheckFingerprint = &fingerprint
	}
	if before.Status.DeclaredNoChange != after.Status.DeclaredNoChange {
		declaredNoChange := after.Status.DeclaredNoChange
		fields.DeclaredNoChange = &declaredNoChange
	}
	if before.Status.LifecycleReportRetryToken != after.Status.LifecycleReportRetryToken {
		token := after.Status.LifecycleReportRetryToken
		fields.LifecycleReportRetryToken = &token
	}
	if before.Status.LifecycleReportActiveRetryToken != after.Status.LifecycleReportActiveRetryToken {
		token := after.Status.LifecycleReportActiveRetryToken
		fields.LifecycleReportActiveRetryToken = &token
	}
	if before.Status.Restarts != after.Status.Restarts {
		restarts := after.Status.Restarts
		fields.Restarts = &restarts
	}
	// PublicationPolicy is set-once: emitted only on the empty-to-set
	// transition, like the timestamps above, so a persisted policy can never
	// be rewritten or cleared by a later patch — including one carrying a
	// mutated in-memory copy.
	if before.Status.PublicationPolicy == nil && after.Status.PublicationPolicy != nil {
		fields.PublicationPolicy = after.Status.PublicationPolicy
	}
	if before.Status.TerminalDiagnostic == nil && after.Status.TerminalDiagnostic != nil {
		fields.TerminalDiagnostic = after.Status.TerminalDiagnostic
	}
	// CIPark is the operator-owned park record (#475). It is replaceable,
	// not set-once: the operator rewrites it as the park advances, and an
	// explicit null clears it, because a merge patch leaves an omitted
	// nested object untouched.
	if !reflect.DeepEqual(before.Status.CIPark, after.Status.CIPark) {
		if after.Status.CIPark == nil {
			fields.ClearCIPark()
		} else {
			fields.CIPark = after.Status.CIPark
		}
	}
	// CIRepair is the operator-owned repair context (#494). The record is
	// cleared once the repair attempt it describes has been observed, so a
	// relaunch before that still finds the repair goal it needs. It follows
	// the same explicit-null clear semantics as CIPark.
	if !reflect.DeepEqual(before.Status.CIRepair, after.Status.CIRepair) {
		if after.Status.CIRepair == nil {
			fields.ClearCIRepair()
		} else {
			fields.CIRepair = after.Status.CIRepair
		}
	}
	// CISettlement is the operator-owned pending settlement record (#475). It
	// is recorded when an attempt's outcome is known and cleared only once
	// the source has accepted the settlement — the clear is the proof of
	// delivery. It follows the same explicit-null clear semantics as CIPark.
	if !reflect.DeepEqual(before.Status.CISettlement, after.Status.CISettlement) {
		if after.Status.CISettlement == nil {
			fields.ClearCISettlement()
		} else {
			fields.CISettlement = after.Status.CISettlement
		}
	}
	// CIParkDeferral is the operator-owned pending park-notice record (#475).
	// It is recorded when the park is established and cleared once the source
	// has accepted the notice, or once the opportunity it describes has
	// ended. It follows the same explicit-null clear semantics as CIPark.
	if !reflect.DeepEqual(before.Status.CIParkDeferral, after.Status.CIParkDeferral) {
		if after.Status.CIParkDeferral == nil {
			fields.ClearCIParkDeferral()
		} else {
			fields.CIParkDeferral = after.Status.CIParkDeferral
		}
	}
	// CIParkRelease is the operator-owned pending deferral-release record
	// (#534). It follows the CISettlement's explicit-null clear semantics:
	// recorded before the close is attempted and cleared only once the close
	// resolves at the source — accepted or moot.
	if !reflect.DeepEqual(before.Status.CIParkRelease, after.Status.CIParkRelease) {
		if after.Status.CIParkRelease == nil {
			fields.ClearCIParkRelease()
		} else {
			fields.CIParkRelease = after.Status.CIParkRelease
		}
	}
	// RetryRequest is the operator-owned explicit-retry record (#97/#606). It is
	// replaceable across request lifetimes and never cleared back to nil — a
	// settled record is durable audit state, and a fresh request replaces it
	// wholesale — so it needs no explicit-null clear path. The record's own
	// serialization always carries reason and settledAt, so a replacement clears a
	// predecessor's stale sub-keys under a JSON merge patch.
	if !reflect.DeepEqual(before.Status.RetryRequest, after.Status.RetryRequest) {
		fields.RetryRequest = after.Status.RetryRequest
	}
	// ControlRequest is replaceable, not set-once: a changed value replaces
	// the record and a cleared record emits an explicit null, because a merge
	// patch leaves an omitted nested object untouched.
	if !reflect.DeepEqual(before.Status.ControlRequest, after.Status.ControlRequest) {
		if after.Status.ControlRequest == nil {
			fields.ClearControlRequest()
		} else {
			if err := fields.SetControlRequest(*after.Status.ControlRequest); err != nil {
				return err
			}
		}
	}
	// ControlRequestAck is operator-cleared only (relaunch and
	// terminalization disarm the control pair); the harness writes records
	// through the broker's trusted status path.
	if !reflect.DeepEqual(before.Status.ControlRequestAck, after.Status.ControlRequestAck) {
		if after.Status.ControlRequestAck == nil {
			fields.ClearControlRequestAck()
		} else {
			if err := fields.SetControlRequestAck(*after.Status.ControlRequestAck); err != nil {
				return err
			}
		}
	}
	if !reflect.DeepEqual(before.Status.Conditions, after.Status.Conditions) {
		if err := fields.SetConditions(after.Status.Conditions); err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(before.Status.Telemetry, after.Status.Telemetry) {
		fields.Telemetry = after.Status.Telemetry
	}
	if reflect.DeepEqual(fields, status.OperatorPatch{}) {
		return nil
	}
	writer := status.NewOperatorWriter(r.statusWriter())
	if err := writer.Patch(ctx, client.ObjectKeyFromObject(after), fields); err != nil {
		return err
	}
	// Metrics are recorded only after a successful status write, matching the
	// terminal-observation contract: a failed patch must not record anything
	// (see TestTerminalStatusPatchFailureRecordsNoMetrics).
	r.recordStatusMetrics(before, after, now)
	return nil
}

// recordStatusMetrics derives the transition-path observability signals from
// the same status diff the patch applied. It runs only after the patch
// succeeded, so a failed write never records a signal the persisted status
// does not back. The phase duration is observed for the phase the run left,
// and only when a durable PhaseEnteredAt marker existed for it — a run whose
// previous phase had no marker (a pre-upgrade run, or any run's first
// transition into a phase, such as Pending) records its transition but no
// fabricated duration.
func (r *CoderRunReconciler) recordStatusMetrics(before, after *courierv1alpha1.CoderRun, now time.Time) {
	rec := r.metrics()
	if before.Status.Phase != after.Status.Phase {
		if before.Status.Phase != "" && after.Status.Phase != "" {
			rec.ObservePhaseTransition(after.Spec.Lane, after.Spec.Mode, before.Status.Phase, after.Status.Phase)
			switch {
			case before.Status.Phase == courierv1alpha1.PhaseVerifying &&
				after.Status.Phase == courierv1alpha1.PhaseAwaitingReview:
				rec.ObserveVerificationOutcome(after.Spec.Lane, after.Spec.Mode, verificationOutcomeGreen)
			case before.Status.Phase == courierv1alpha1.PhaseVerifying &&
				(after.Status.Phase == courierv1alpha1.PhaseFailed || after.Status.Phase == courierv1alpha1.PhaseHandedOff):
				rec.ObserveVerificationOutcome(after.Spec.Lane, after.Spec.Mode, verificationOutcomeRed)
			case before.Status.Phase == courierv1alpha1.PhaseVerifying &&
				after.Status.Phase == courierv1alpha1.PhaseNeedsHuman:
				rec.ObserveVerificationOutcome(after.Spec.Lane, after.Spec.Mode, verificationOutcomeUnobservable)
			}
		}
		if before.Status.PhaseEnteredAt != nil && !before.Status.PhaseEnteredAt.IsZero() {
			rec.ObservePhaseDuration(after.Spec.Lane, after.Spec.Mode,
				before.Status.Phase, now.Sub(before.Status.PhaseEnteredAt.Time))
		}
	}
	if after.Status.Restarts > before.Status.Restarts {
		rec.ObserveInfraRestarts(after.Spec.Lane, after.Spec.Mode, after.Status.Restarts-before.Status.Restarts)
	}
	// Telemetry can land after the first terminal transition (the harness
	// termination handoff patches status later), so tokens are counted as
	// per-kind deltas on every telemetry change: a nil→set telemetry with
	// values yields exactly the full amounts, later growth yields increments,
	// and an identical re-patch yields nothing. The derived Total is never
	// emitted.
	if !reflect.DeepEqual(before.Status.Telemetry, after.Status.Telemetry) &&
		after.Status.Telemetry != nil && after.Status.Telemetry.Tokens != nil {
		var beforeTokens courierv1alpha1.TokenUsage
		if before.Status.Telemetry != nil && before.Status.Telemetry.Tokens != nil {
			beforeTokens = *before.Status.Telemetry.Tokens
		}
		afterTokens := after.Status.Telemetry.Tokens
		pairs := []struct {
			kind          string
			after, before int64
		}{
			{tokenKinds[0], afterTokens.Input, beforeTokens.Input},
			{tokenKinds[1], afterTokens.Output, beforeTokens.Output},
			{tokenKinds[2], afterTokens.Reasoning, beforeTokens.Reasoning},
			{tokenKinds[3], afterTokens.CacheRead, beforeTokens.CacheRead},
			{tokenKinds[4], afterTokens.CacheWrite, beforeTokens.CacheWrite},
		}
		for _, p := range pairs {
			if d := p.after - p.before; d > 0 {
				rec.ObserveTokenDelta(after.Spec.Lane, after.Spec.Mode, p.kind, d)
			}
		}
	}
}

// reader returns the reader world reads go through, preferring the injected
// API reader and falling back to the cached client. SetupWithManager requires
// APIReader in production, so the fallback serves only direct-construction
// tests and the pod-loss confirmation never runs against cached state.
func (r *CoderRunReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *CoderRunReconciler) statusWriter() status.PatchWriter {
	if r.StatusWriter != nil {
		return r.StatusWriter
	}
	return status.KubePatchWriter{Client: r.Client}
}

// hasCoordinatorContainer reports whether pod declares the coordinator
// container. The legacy coordinator pod and the secure topology's control pod
// both name it that way, while its worker and broker pods do not.
func hasCoordinatorContainer(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		if container.Name == coordinatorContainerName {
			return true
		}
	}
	return false
}

// coordinatorPodOf returns the run's coordinator pod among pods: a pod owned
// by, or labeled for, the run that carries the coordinator container. A run
// whose control pod vanished is therefore recognized even while the rest of a
// secure topology survives. A coordinator that is being deleted is neither
// the live coordinator nor a confirmed loss, so the caller hands the
// judgement to the backstop's live read.
func coordinatorPodOf(pods []corev1.Pod, run *courierv1alpha1.CoderRun) *corev1.Pod {
	for i := range pods {
		pod := &pods[i]
		if !podBelongsToRun(pod, run) || !hasCoordinatorContainer(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		return pod
	}
	return nil
}

// coordinatorPodUnrecoverable reports whether pod is a terminal-phase object
// (Failed or Succeeded) that records no coordinator termination status — an
// Evicted pod or one that failed scheduling, the shapes Kubernetes leaves
// when it never started or never reported the container. No exit code was
// ever reported, so no model outcome can be inferred from such an object:
// it is an unobservable dead coordinator, the same infrastructure loss as a
// vanished pod.
func coordinatorPodUnrecoverable(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
		return false
	}
	_, _, _, _, terminated := coordinatorTermination(pod)
	return !terminated
}

func podBelongsToRun(pod *corev1.Pod, run *courierv1alpha1.CoderRun) bool {
	if pod == nil || run == nil || pod.Namespace != run.Namespace {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.Name == run.Name && owner.Kind == "CoderRun" {
			return true
		}
	}
	// Three run-label schemes are accepted: the digest-bounded executor and
	// topology values, which agree by construction for every name, and the
	// legacy pre-digest truncation that pods created before the digest
	// scheme carry until they are replaced.
	label := pod.Labels[executor.LabelRun]
	return label == executor.RunLabelValue(run.Name) ||
		label == topology.RunLabelValue(run.Name) ||
		label == executor.LegacyRunLabelValue(run.Name)
}

type terminationMessage struct {
	Phase           string                                   `json:"phase"`
	Result          string                                   `json:"result"`
	ExitCode        int32                                    `json:"exit_code"`
	Reason          string                                   `json:"reason"`
	Outcome         string                                   `json:"outcome"`
	ResultKind      string                                   `json:"result_kind,omitempty"`
	Category        string                                   `json:"category,omitempty"`
	Summary         *courierv1alpha1.RunTelemetry            `json:"summary"`
	WorkspaceSafety *courierv1alpha1.WorkspaceSafetyEvidence `json:"workspace_safety,omitempty"`
}

func boundedWorkspaceSafety(in *courierv1alpha1.WorkspaceSafetyEvidence) *courierv1alpha1.WorkspaceSafetyEvidence {
	if in == nil || (in.State != "clean" && in.State != "dirty" && in.State != "empty" && in.State != "unknown" && in.State != "pushed") {
		return nil
	}
	bound := func(value string, max int) string {
		if len(value) > max {
			return ""
		}
		return value
	}
	return &courierv1alpha1.WorkspaceSafetyEvidence{State: in.State, Branch: bound(in.Branch, 256), Head: bound(in.Head, 128), RemoteHead: bound(in.RemoteHead, 128)}
}

func validTerminationPhaseResult(exitCode int32, phase, result string) bool {
	switch exitCode {
	case 0:
		return phase == "Verifying" && result == "success"
	case 2:
		return phase == "NeedsHuman" && result == "needs-human"
	case exitDeclaredNoChange:
		return phase == "NoChangeNeeded" && result == "success"
	default:
		return phase == "Failed" && result == "failure"
	}
}

// coordinatorContainerStart returns the coordinator container's start time as
// the pod's status records it: the Running state while the container is up,
// the Terminated state once it has exited. A zero result means the pod never
// reached a recorded start, which leaves StartedAt unset.
func coordinatorContainerStart(pod *corev1.Pod) time.Time {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != coordinatorContainerName {
			continue
		}
		if cs.State.Running != nil {
			return cs.State.Running.StartedAt.Time
		}
		if cs.State.Terminated != nil {
			return cs.State.Terminated.StartedAt.Time
		}
	}
	return time.Time{}
}

// durationString renders a duration in Go format, clamping a negative value
// (clock skew between the kubelet and the apiserver) to zero.
func durationString(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return d.Round(time.Second).String()
}

const handoffPrefix = "COURIER_TERMINATION "

func parseCoordinatorTermination(pod *corev1.Pod) (terminationMessage, bool) {
	if pod == nil {
		return terminationMessage{}, false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != coordinatorContainerName || status.State.Terminated == nil {
			continue
		}
		terminated := status.State.Terminated
		var message terminationMessage
		rawMessage := strings.TrimSpace(terminated.Message)
		payload, ok := strings.CutPrefix(rawMessage, handoffPrefix)
		if !ok || json.Unmarshal([]byte(payload), &message) != nil || message.ExitCode != terminated.ExitCode || !validTerminationPhaseResult(terminated.ExitCode, message.Phase, message.Result) {
			return terminationMessage{}, false
		}
		return message, true
	}
	return terminationMessage{}, false
}

func coordinatorHandoffCategory(pod *corev1.Pod) string {
	message, valid := parseCoordinatorTermination(pod)
	if !valid {
		return ""
	}
	return strings.TrimSpace(message.Category)
}

func coordinatorHandoffValid(pod *corev1.Pod) bool {
	_, valid := parseCoordinatorTermination(pod)
	return valid
}

func coordinatorTermination(pod *corev1.Pod) (int32, string, string, *courierv1alpha1.RunTelemetry, bool) {
	if pod == nil {
		return 0, "", "", nil, false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != coordinatorContainerName || status.State.Terminated == nil {
			continue
		}
		terminated := status.State.Terminated
		code, reason, outcome := terminated.ExitCode, "", ""
		var summary *courierv1alpha1.RunTelemetry
		if message, valid := parseCoordinatorTermination(pod); valid {
			reason, outcome = strings.TrimSpace(message.Reason), strings.TrimSpace(message.Outcome)
			summary = message.Summary
		}
		return code, reason, outcome, summary, true
	}
	return 0, "", "", nil, false
}

// exitDeclaredNoChange is the executor's verified no_change_needed exit.
const exitDeclaredNoChange = 3

// outcomeBlockedExternal is the executor's declared external-prerequisite
// outcome, carried in the trusted termination handoff on exit 2.
const outcomeBlockedExternal = "blocked_external"

// exitNeedsHuman is the executor's NeedsHuman exit: a declared decision or
// external block, or a guard refusal.
const exitNeedsHuman = 2

// trustedForgeUnavailableHandoff reports the exact #455 pairing that shares
// the capacity-exhausted workspace-safety evidence gate: the executor
// declared blocked_external while the same validated termination handoff
// carries the forge-unavailable diagnostic category — the transient
// forge-read ending where the world was never observed and the published
// work must stay recoverable. Any other blocked_external declaration is a
// task-shaped prerequisite block and never opens this gate.
func trustedForgeUnavailableHandoff(pod *corev1.Pod, terminationOutcome string) bool {
	return terminationOutcome == string(executor.OutcomeBlockedExternal) &&
		coordinatorHandoffCategory(pod) == string(courierv1alpha1.TerminalDiagnosticCategoryForgeUnavailable)
}

// outcomeCapacityExhausted is the executor's affirmative structured
// declaration that execution capacity was confirmed exhausted (#320/#327).
// It arrives only through the trusted termination handoff — never inferred
// from transport status — and routes the run to the recoverable
// BlockedExternal capacity path instead of a failure or a human park.
const outcomeCapacityExhausted = "capacity_exhausted"

// phaseForExit maps the executor's exit code and declared outcome to the run's
// next phase. A fix-pr no_change_needed declaration is settled against the
// world, not the coordinator's word: it is routed to Verifying, which reaches
// Done only when the PR is already merged and AwaitingReview only after stable
// green checks. A blocked_external declaration becomes a recoverable
// dependency block; a needs_decision declaration (or any exit 2 without a
// recognized external-block outcome) remains NeedsHuman. Other failure exits
// remain Failed. This is the legacy classification contract: a native run
// whose trusted terminal result exists is classified by
// phaseForNativeResult instead (#127).
func phaseForExit(mode courierv1alpha1.Mode, exitCode int32, outcome string) courierv1alpha1.Phase {
	switch exitCode {
	case 0:
		return courierv1alpha1.PhaseVerifying
	case 2:
		if outcome == outcomeBlockedExternal || outcome == outcomeCapacityExhausted {
			// A confirmed capacity exhaustion is a recoverable wait on an
			// external resource, not a verdict on the work and not a human
			// park; the diagnostic category keeps it distinguishable from a
			// task-shaped external block (#327).
			return courierv1alpha1.PhaseBlockedExternal
		}
		return courierv1alpha1.PhaseNeedsHuman
	case exitDeclaredNoChange:
		if mode == courierv1alpha1.ModeFixPR {
			return courierv1alpha1.PhaseVerifying
		}
		return courierv1alpha1.PhaseAwaitingReview
	default:
		return courierv1alpha1.PhaseFailed
	}
}

// nativeResultKindOf reports the trusted native terminal result kind a valid
// termination handoff carries (#127), or empty when the message carries none
// or an unrecognized one — an unknown kind is never a classification.
func nativeResultKindOf(message terminationMessage) courierv1alpha1.NativeResultKind {
	switch kind := courierv1alpha1.NativeResultKind(strings.TrimSpace(message.ResultKind)); kind {
	case courierv1alpha1.NativeResultPublished, courierv1alpha1.NativeResultNoChange,
		courierv1alpha1.NativeResultNeedsHuman, courierv1alpha1.NativeResultBlockedExternal,
		courierv1alpha1.NativeResultCapacityExhausted, courierv1alpha1.NativeResultWorkloadFailed,
		courierv1alpha1.NativeResultInfrastructureFailed:
		return kind
	default:
		return ""
	}
}

// trustedNativeResult returns the trusted native coordinator terminal result
// that classifies this coordinator pod's termination, when one exists (#127).
// The persisted status result is authoritative when it fences to this pod's
// UID — the fence that keeps a stale incarnation's result from classifying a
// replacement's termination; otherwise the pod's own validated termination
// handoff carries the kind as the fallback channel for a status write that
// never landed. A run with neither has no trusted result and keeps the legacy
// exit-code classification.
func trustedNativeResult(run *courierv1alpha1.CoderRun, pod *corev1.Pod) (courierv1alpha1.NativeResult, bool) {
	if pod == nil {
		return courierv1alpha1.NativeResult{}, false
	}
	if result := run.Status.NativeResult; result != nil &&
		result.CoordinatorPodUID == string(pod.UID) && nativeResultKindOf(terminationMessage{ResultKind: string(result.Kind)}) != "" {
		return *result, true
	}
	if message, valid := parseCoordinatorTermination(pod); valid {
		if kind := nativeResultKindOf(message); kind != "" {
			return courierv1alpha1.NativeResult{Kind: kind, Reason: strings.TrimSpace(message.Reason)}, true
		}
	}
	return courierv1alpha1.NativeResult{}, false
}

// phaseForNativeResult maps the trusted native terminal result onto the run's
// next phase (#127). The routes mirror the legacy exit-code mapping exactly —
// a published or no-change result still only hands the run to the operator's
// own world verification, never to a successful AwaitingReview on the
// coordinator's word. An infrastructure failure is not mapped here: it is the
// relaunch/resume case the caller routes through the crashloop-bounded
// relaunch before any terminal phase is chosen (HARNESS.md §8).
func phaseForNativeResult(mode courierv1alpha1.Mode, kind courierv1alpha1.NativeResultKind) courierv1alpha1.Phase {
	switch kind {
	case courierv1alpha1.NativeResultPublished:
		return courierv1alpha1.PhaseVerifying
	case courierv1alpha1.NativeResultNoChange:
		if mode == courierv1alpha1.ModeFixPR {
			return courierv1alpha1.PhaseVerifying
		}
		return courierv1alpha1.PhaseAwaitingReview
	case courierv1alpha1.NativeResultNeedsHuman:
		return courierv1alpha1.PhaseNeedsHuman
	case courierv1alpha1.NativeResultBlockedExternal, courierv1alpha1.NativeResultCapacityExhausted:
		return courierv1alpha1.PhaseBlockedExternal
	default:
		// workload_failed and any unmapped kind fail closed onto the terminal
		// failure verdict on the work.
		return courierv1alpha1.PhaseFailed
	}
}

// infrastructureFailureReason is the human-facing cause a trusted
// infrastructure-failure relaunch event carries (#127).
const infrastructureFailureEventReason = "coordinator reported an infrastructure failure"

// infrastructureFailureCeilingReason is the human-facing cause the ceiling
// hand-off carries when trusted infrastructure failures accumulated to the
// restart ceiling (#127).
const infrastructureFailureCeilingReason = "coordinator infrastructure failures reached the infrastructure restart ceiling"

// relaunchInfrastructureFailure is the recovery route for a coordinator that
// ended with a trusted infrastructure-failure result (#127): the
// infrastructure failed around the work — a model gateway or stream failure,
// a status write that will not land, a publication transport failure — so the
// ending is never a terminal verdict on the work. The run relaunches and
// resumes from durable state exactly like pod loss, bounded by the same
// crashloop ceiling, after the terminated pod is deleted so the
// deterministic name is free for the replacement. At the ceiling the run is
// handed to a human and the terminated pod is preserved as the ending's only
// record of the cause, exactly as the wedge and pod-loss paths preserve an
// unrecoverable object. A delete that conflicts re-observes instead of
// charging: the next reconcile re-classifies the same terminated pod.
func (r *CoderRunReconciler) relaunchInfrastructureFailure(ctx context.Context, run *courierv1alpha1.CoderRun, pod *corev1.Pod, trusted courierv1alpha1.NativeResult) (ctrl.Result, error) {
	// For secure runs the surviving round — the untrusted worker and the
	// credentialed broker — must not outlive the supervisor: the loss is
	// confirmed by the trusted result, so the fence runs on this reconcile
	// exactly as the pod-loss paths do (#215).
	if r.Secure != nil {
		if err := r.Secure.Fence(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	diagnostic := buildTerminalDiagnostic(r.clock(), nil, "", "", "", false, pod, trusted.Kind)
	if run.Status.Restarts < r.maxRestarts() {
		if err := r.deleteObservedPod(ctx, pod); err != nil {
			if apierrors.IsConflict(err) {
				// The pod changed between observation and deletion: re-observe
				// instead of acting on a world that no longer exists.
				return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
			}
			return ctrl.Result{}, err
		}
		present, err := coordinatorObjectPresentLive(ctx, r.reader(), run)
		if err != nil || present {
			// The deterministic name is not provably free yet; the charge is
			// deferred until a live read shows the object gone, so one
			// physical ending is charged exactly once.
			return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
		}
	}
	return r.relaunchAfterInfraLoss(ctx, run, infrastructureFailureCeilingReason, infrastructureFailureEventReason, diagnostic)
}
