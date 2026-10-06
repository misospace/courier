package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
	"github.com/misospace/courier/internal/topology"
)

// capacityRequeueDelay bounds how long a Pending run can wait behind a full
// lane before checking again.
const (
	capacityRequeueDelay        = 15 * time.Second
	observationRequeueDelay     = 5 * time.Second
	lifecycleReportRequeueDelay = 30 * time.Second
)

// coordinatorContainerName is the coordinator's container name, shared by the
// legacy single-pod topology and the secure topology's control pod.
const coordinatorContainerName = "coordinator"

// lifecycleReportedCondition tracks whether a terminal run's source lifecycle
// report has been published; a False status means a later reconcile retries it.
const (
	lifecycleReportedCondition                              = "LifecycleReported"
	lifecycleReportPendingReason                            = "Pending"
	lifecycleReportPendingBlockedReportParksPRFixReason     = "PendingBlockedReportParksPRFix"
	lifecycleReportPendingWithErrorReason                   = "PendingWithError"
	lifecycleReportPendingExternalVerificationFailureReason = "PendingExternalVerificationFailure"
	externalVerificationFailureReason                       = "external verification failed"
)

type terminalLifecycleIntent struct {
	skipPRFixQueueMark bool
	ciFailure          bool
	error              string
}

// CoderRunReconciler reconciles a CoderRun object.
type CoderRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// APIReader reads the API server directly, bypassing the informer cache.
	// The pod-loss backstop uses it to confirm a missing coordinator really is
	// gone before charging a relaunch: a cache that has not caught up must not
	// be mistaken for infrastructure loss, and infrastructure loss must not be
	// hidden by a cache that never delivers the pod. It is required in
	// production — SetupWithManager rejects a nil reader so the pod-loss
	// confirmation can never fall back to cached state; the reader()
	// fallback serves only direct-construction tests.
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

	// Events optionally emits structured run events as JSON lines for the
	// deployment's log collection stack. Nil disables emission; a failed
	// emission never fails reconciliation.
	Events *courierlog.Emitter

	// DoneRetention is how long a resolved Done run is kept before deletion.
	// Zero uses the package default.
	DoneRetention time.Duration

	// LivenessWindow is how long a Running run may go without a heartbeat before
	// it is considered wedged and reaped. Zero uses the package default.
	LivenessWindow time.Duration

	// MaxRestarts is the crashloop threshold: after this many infra relaunches a
	// run transitions to NeedsHuman. Zero uses the package default.
	MaxRestarts int

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
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=laneprofiles,verbs=get;list;watch
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
	// the run already reached its phase and freed capacity.
	if run.Status.Phase != "" && run.Status.Phase != courierv1alpha1.PhasePending {
		if lifecycleReportPending(&run) {
			return r.retryTerminalLifecycle(ctx, &run)
		}
		l.V(1).Info("reconcile", "phase", run.Status.Phase)
		return ctrl.Result{}, nil
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

	var runs courierv1alpha1.CoderRunList
	if err := r.List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
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

	if err := adapter.Claim(ctx, item); err != nil {
		return ctrl.Result{}, err
	}
	// Claimed is the admission reservation. Persist it before launching so a
	// concurrent reconciliation cannot observe an unaccounted-for launch.
	before := run.DeepCopy()
	run.Status.Phase = courierv1alpha1.PhaseClaimed
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
				return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
			}
			if result.Requeue || result.RequeueAfter > 0 {
				// Provisioning is in flight; the run stays Claimed and the
				// next reconcile resumes it.
				return result, nil
			}
		} else if err := r.Launch(ctx, &run); err != nil {
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

// resumeClaimed closes the small crash/conflict window between reserving a run
// and recording that its pod is Running. Launch is idempotent at the Kubernetes
// resource boundary (AlreadyExists is accepted by CoordinatorLauncher), so a
// retry can safely finish a partially-completed admission.
func (r *CoderRunReconciler) resumeClaimed(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
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
			return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
		}
		if result.Requeue || result.RequeueAfter > 0 {
			return result, nil
		}
	} else if err := r.Launch(ctx, run); err != nil {
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
		return errors.New("coderun controller: APIReader is required so the pod-loss confirmation reads the API server directly and never falls back to cached state")
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
	adapter, err := r.Sources.lookup(run.Spec.Source)
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
func (r *CoderRunReconciler) rejectClaim(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, cause error) error {
	releaseErr := adapter.Release(ctx, item)
	if errors.Is(cause, source.ErrStaleWork) {
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

// terminateMissingHead ends a fix-pr run whose pull request head repository is
// gone (a deleted fork). Unlike a transient resolution failure, the condition
// cannot recover by re-claiming, so the claim is released and the run
// terminalizes NeedsHuman rather than cycling claim and release on every
// reconcile. The cause is handled here, so it is logged and not returned as a
// reconcile error.
func (r *CoderRunReconciler) terminateMissingHead(ctx context.Context, run *courierv1alpha1.CoderRun, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("terminating fix-pr run: pull request head repository is missing",
		"run", run.Name, "branch", run.Status.Branch, "cause", cause.Error())
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	releaseErr := adapter.Release(ctx, item)
	run.Status.Branch = ""
	run.Status.HeadRepo = ""
	run.Status.HeadSHA = ""
	terminalResult, terminalErr := r.transitionTerminal(ctx, run, courierv1alpha1.PhaseNeedsHuman, "", terminalLifecycleIntent{}, nil)
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
	// The secure topology's operator-owned observation runs before the
	// coordinator-termination handler below: a terminated control pod must
	// fence its worker and take the broker down in the same reconcile that
	// terminalizes the run. Terminal runs are never reaped, so fencing left
	// to a later reconcile would never run at all — the untrusted executor
	// and the credentialed broker would outlive their supervisor.
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
		phase := phaseForExit(run.Spec.Mode, exitCode)
		if phase == courierv1alpha1.PhaseVerifying {
			before := run.DeepCopy()
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
		intent := terminalLifecycleIntent{
			skipPRFixQueueMark: exitCode == exitDeclaredNoChange && run.Spec.Mode == courierv1alpha1.ModeFixPR,
		}
		if exitCode == 2 && phase == courierv1alpha1.PhaseNeedsHuman && terminationOutcome == "blocked_external" {
			intent.error = terminationReason
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
		return r.transitionTerminal(ctx, run, courierv1alpha1.PhaseDone, pr, terminalLifecycleIntent{}, nil)
	}
	if state == observationFailed {
		// Queue-backed fix-pr work has a source-owned retry policy. Keep
		// resolve-issue observers alive so this run can carry the original issue
		// to review after the PR is repaired by a later run.
		if run.Spec.Mode != courierv1alpha1.ModeFixPR {
			before := run.DeepCopy()
			if pr != "" {
				run.Status.PR = pr
			}
			run.Status.CheckFingerprint = ""
			if err := r.patchStatus(ctx, before, run); err != nil {
				return ctrl.Result{}, err
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
	// way. A real failure is recognized immediately and skips the settle; it
	// needs no second observation.
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
	if err := r.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, err
	}
	if settled {
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
	if phase == courierv1alpha1.PhaseNeedsHuman || phase == courierv1alpha1.PhaseFailed {
		if pr == "" {
			pr = r.enrichTerminalPR(ctx, run)
		}
	}
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	phaseChanged := run.Status.Phase != phase
	before := run.DeepCopy()
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
		// operator-marked Done (post-merge, currently unimplemented) never
		// moves it.
		if run.Status.FinishedAt == nil {
			run.Status.FinishedAt = &metav1.Time{Time: r.clock().UTC()}
			if run.Status.StartedAt != nil && run.Status.RunDuration == "" {
				run.Status.RunDuration = durationString(run.Status.FinishedAt.Time.Sub(run.Status.StartedAt.Time))
			}
		}
		// Persist report intent with the phase so retries can reproduce the same
		// lifecycle without extending the CRD status schema.
		r.setLifecycleReport(run, false, pendingLifecycleReason(intent), intent.error)
	}
	// Write the terminal phase first so the run stops holding lane capacity
	// regardless of whether the source lifecycle report below succeeds. The
	// run's own terminalization must never depend on the source.
	if run.Status.Phase != before.Status.Phase || run.Status.PR != before.Status.PR || !reflect.DeepEqual(run.Status.Telemetry, before.Status.Telemetry) {
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
	// The terminal phase is final, so only the reconcile that changes into it
	// records the run's metrics; re-reconciles return above.
	r.metrics().ObserveTerminal(run)
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
// of the same work: the report is dropped, not retried. A transient failure
// leaves a pending report that a later reconcile retries; the source's
// idempotency key makes the retry safe. A permanent failure retries at the
// bounded interval, mirroring how waiting-for-capacity requeues work — the run
// itself is never held by the report.
func (r *CoderRunReconciler) publishTerminalLifecycle(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, phase courierv1alpha1.Phase, intent terminalLifecycleIntent) (ctrl.Result, error) {
	err := r.publishSourceLifecycle(ctx, run, adapter, item, phase, intent)
	switch {
	case err == nil:
		return ctrl.Result{}, r.markLifecycleReported(ctx, run, true, "Published", "")
	case errors.Is(err, source.ErrSuperseded):
		log.FromContext(ctx).Info("dropping superseded source lifecycle report",
			"run", run.Name, "phase", phase, "error", err.Error())
		return ctrl.Result{}, r.markLifecycleReported(ctx, run, true, "Superseded", err.Error())
	default:
		log.FromContext(ctx).Info("terminal lifecycle report failed; will retry",
			"run", run.Name, "phase", phase, "error", err.Error())
		// Keep the explanation in the pending condition so a declared block's
		// lifecycle report can be reproduced exactly on every retry.
		return ctrl.Result{RequeueAfter: lifecycleReportRequeueDelay}, r.markLifecycleReported(ctx, run, false, pendingLifecycleReason(intent), intent.error)
	}
}

func (r *CoderRunReconciler) publishSourceLifecycle(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, phase courierv1alpha1.Phase, intent terminalLifecycleIntent) error {
	state := stateForTerminalPhase(phase, run.Spec.Mode, intent)
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
	return ""
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

// markLifecycleReported records whether the terminal lifecycle report for the
// run's current phase has been published. Published means done or intentionally
// dropped (superseded); False means a later reconcile must retry it.
func (r *CoderRunReconciler) markLifecycleReported(ctx context.Context, run *courierv1alpha1.CoderRun, published bool, reason, message string) error {
	before := run.DeepCopy()
	r.setLifecycleReport(run, published, reason, message)
	return r.patchStatus(ctx, before, run)
}

// lifecycleReportPending reports a terminal run whose lifecycle report has not
// yet been published and must be retried. Runs that reached a terminal phase
// through another path, or that predate this condition, are not pending.
func lifecycleReportPending(run *courierv1alpha1.CoderRun) bool {
	switch run.Status.Phase {
	case courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseFailed:
	default:
		return false
	}
	cond := apimeta.FindStatusCondition(run.Status.Conditions, lifecycleReportedCondition)
	return cond != nil && cond.Status == metav1.ConditionFalse
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
	case courierv1alpha1.PhaseNeedsHuman:
		lifecycle.Result = source.ResultBlocked
		lifecycle.Error = intent.error
		if lifecycle.Error == "" {
			lifecycle.Error = "run requires human intervention"
		}
		lifecycle.BlockedReportParksPRFix = intent.skipPRFixQueueMark
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
	var fields status.OperatorPatch
	if before.Status.Phase != after.Status.Phase {
		fields.Phase = after.Status.Phase
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
	if !reflect.DeepEqual(before.Status.Conditions, after.Status.Conditions) {
		fields.Conditions = after.Status.Conditions
	}
	if !reflect.DeepEqual(before.Status.Telemetry, after.Status.Telemetry) {
		fields.Telemetry = after.Status.Telemetry
	}
	if reflect.DeepEqual(fields, status.OperatorPatch{}) {
		return nil
	}
	writer := status.NewOperatorWriter(r.statusWriter())
	return writer.Patch(ctx, client.ObjectKeyFromObject(after), fields)
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
	return pod.Labels[executor.LabelRun] == run.Name
}

type terminationMessage struct {
	Phase    string                        `json:"phase"`
	Result   string                        `json:"result"`
	ExitCode int32                         `json:"exit_code"`
	Reason   string                        `json:"reason"`
	Outcome  string                        `json:"outcome"`
	Summary  *courierv1alpha1.RunTelemetry `json:"summary"`
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

func coordinatorTermination(pod *corev1.Pod) (int32, string, string, *courierv1alpha1.RunTelemetry, bool) {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != coordinatorContainerName || status.State.Terminated == nil {
			continue
		}
		terminated := status.State.Terminated
		code, reason, outcome := terminated.ExitCode, "", ""
		var summary *courierv1alpha1.RunTelemetry
		var message terminationMessage
		rawMessage := strings.TrimSpace(terminated.Message)
		const handoffPrefix = "COURIER_TERMINATION "
		if payload, ok := strings.CutPrefix(rawMessage, handoffPrefix); ok && json.Unmarshal([]byte(payload), &message) == nil && message.ExitCode == code && validTerminationPhaseResult(code, message.Phase, message.Result) {
			reason, outcome = strings.TrimSpace(message.Reason), strings.TrimSpace(message.Outcome)
			summary = message.Summary
		}
		return code, reason, outcome, summary, true
	}
	return 0, "", "", nil, false
}

// exitDeclaredNoChange is the executor's verified no_change_needed exit.
const exitDeclaredNoChange = 3

// phaseForExit maps the executor's exit code to the run's next phase. A
// no-change declaration is reviewable in resolve-issue mode; fix-pr remains
// NeedsHuman until Dispatch supports an explicit already-addressed settlement.
func phaseForExit(mode courierv1alpha1.Mode, exitCode int32) courierv1alpha1.Phase {
	switch exitCode {
	case 0:
		return courierv1alpha1.PhaseVerifying
	case 2:
		return courierv1alpha1.PhaseNeedsHuman
	case exitDeclaredNoChange:
		if mode == courierv1alpha1.ModeFixPR {
			return courierv1alpha1.PhaseNeedsHuman
		}
		return courierv1alpha1.PhaseAwaitingReview
	default:
		return courierv1alpha1.PhaseFailed
	}
}
