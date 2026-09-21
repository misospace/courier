package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

// capacityRequeueDelay bounds how long a Pending run can wait behind a full
// lane before checking again.
const (
	capacityRequeueDelay    = 15 * time.Second
	observationRequeueDelay = 5 * time.Second
)

// CoderRunReconciler reconciles a CoderRun object.
type CoderRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Launch is injected by the coordinator launcher. It is optional while
	// launch support is being assembled; admission still reserves capacity by
	// moving Pending runs to Claimed.
	Launch LaunchFunc

	// Sources resolves the adapter named by CoderRun.Spec.Source. WorkItemID is
	// passed through unchanged for every claim/release/transition call.
	Sources *SourceRegistry

	// PRHeadResolver is required for fix-pr runs. It returns the branch attached
	// to the existing PR; no branch is synthesized from the PR number.
	PRHeadResolver ExistingPRHeadResolver

	// StatusWriter is the status transport used for operator-owned fields.
	// Nil falls back to server-side apply through the reconciler's client.
	StatusWriter status.PatchWriter

	// Observer reads the external pull request and CI state after a successful run.
	Observer WorldObserver
}

// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderuns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderuns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderuns/finalizers,verbs=update
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=laneprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete

// Reconcile is the CoderRun control loop.
//
// Pending runs are admitted against the lane's current Claimed + Running
// count. Claiming happens before launch so the claim itself reserves capacity;
// a failed launch releases that reservation and leaves the run retryable.
func (r *CoderRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var run courierv1alpha1.CoderRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if run.Status.Phase == courierv1alpha1.PhaseRunning {
		return r.observeRunning(ctx, &run)
	}
	if run.Status.Phase == courierv1alpha1.PhaseClaimed {
		return r.resumeClaimed(ctx, &run)
	}
	if run.Status.Phase == courierv1alpha1.PhaseDone {
		return ctrl.Result{}, r.resolveCompleted(ctx, &run)
	}
	// Status is initially empty on a newly-created CoderRun. Treat that zero
	// value as Pending so a source does not need a second status write before
	// admission can begin. Other terminal phases are left unchanged.
	if run.Status.Phase != "" && run.Status.Phase != courierv1alpha1.PhasePending {
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
	if err := r.patchStatus(ctx, before, &run); err != nil {
		return ctrl.Result{}, errors.Join(err, adapter.Release(ctx, item))
	}

	resolvedBranch, err := resolveRunBranch(ctx, &run, r.PRHeadResolver)
	if err != nil {
		return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
	}
	before = run.DeepCopy()
	run.Status.Branch = resolvedBranch
	if err := r.patchStatus(ctx, before, &run); err != nil {
		return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
	}

	launched := r.Launch != nil
	if launched {
		if err := adapter.Transition(ctx, item, source.StateInProgress); err != nil {
			return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
		}
		beforeLaunch := run.DeepCopy()
		if err := r.Launch(ctx, &run); err != nil {
			return ctrl.Result{}, r.releaseClaim(ctx, &run, adapter, item, err)
		}
		run.Status.Phase = courierv1alpha1.PhaseRunning
		if err := r.patchStatus(ctx, beforeLaunch, &run); err != nil {
			return ctrl.Result{}, err
		}
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
		branchName, err := resolveRunBranch(ctx, run, r.PRHeadResolver)
		if err != nil {
			return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
		}
		before := run.DeepCopy()
		run.Status.Branch = branchName
		if err := r.patchStatus(ctx, before, run); err != nil {
			return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
		}
	}
	if err := adapter.Transition(ctx, item, source.StateInProgress); err != nil {
		return ctrl.Result{}, err
	}
	beforeLaunch := run.DeepCopy()
	if err := r.Launch(ctx, run); err != nil {
		return ctrl.Result{}, r.releaseClaim(ctx, run, adapter, item, err)
	}
	run.Status.Phase = courierv1alpha1.PhaseRunning
	if err := r.patchStatus(ctx, beforeLaunch, run); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the controller with the manager.
func (r *CoderRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&courierv1alpha1.CoderRun{}).
		Owns(&corev1.Pod{}).
		Complete(r)
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

func (r *CoderRunReconciler) releaseClaim(ctx context.Context, run *courierv1alpha1.CoderRun, adapter source.Adapter, item source.WorkItem, cause error) error {
	// Attempt both sides of the rollback even if one fails. Returning the
	// joined error keeps the original launch/branch failure visible while
	// exposing a stranded source claim or status update to the controller.
	releaseErr := adapter.Release(ctx, item)
	before := run.DeepCopy()
	run.Status.Phase = courierv1alpha1.PhasePending
	run.Status.Branch = ""
	statusErr := r.patchStatus(ctx, before, run)
	return errors.Join(cause, releaseErr, statusErr)
}

func (r *CoderRunReconciler) resolveCompleted(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return err
	}
	return adapter.Resolve(ctx, item)
}

func (r *CoderRunReconciler) observeRunning(ctx context.Context, run *courierv1alpha1.CoderRun) (ctrl.Result, error) {
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(run.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podBelongsToRun(pod, run) {
			continue
		}
		exitCode, terminated := coordinatorExitCode(pod)
		if !terminated {
			continue
		}
		phase := terminalPhaseForExit(exitCode)
		pr := ""
		if phase == courierv1alpha1.PhaseAwaitingReview {
			if r.Observer == nil {
				phase = courierv1alpha1.PhaseNeedsHuman
			} else {
				observation, err := r.Observer.Observe(ctx, run.Spec.Repo, run.Status.Branch)
				if err != nil {
					return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
				}
				pr = observation.PR
				switch observeState(observation) {
				case observationPending:
					return ctrl.Result{RequeueAfter: observationRequeueDelay}, nil
				case observationPassed:
					phase = courierv1alpha1.PhaseAwaitingReview
				default:
					phase = courierv1alpha1.PhaseNeedsHuman
				}
			}
		}
		var transitionErr error
		switch phase {
		case courierv1alpha1.PhaseAwaitingReview:
			transitionErr = adapter.Transition(ctx, item, source.StateInReview)
		case courierv1alpha1.PhaseNeedsHuman, courierv1alpha1.PhaseFailed:
			transitionErr = adapter.Transition(ctx, item, source.StateNeedsHuman)
		}
		if transitionErr != nil {
			return ctrl.Result{}, transitionErr
		}
		before := run.DeepCopy()
		run.Status.Phase = phase
		if pr != "" {
			run.Status.PR = pr
		}
		if run.Status.Phase != before.Status.Phase || run.Status.PR != before.Status.PR {
			if err := r.patchStatus(ctx, before, run); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

// patchStatus writes only the operator-owned status fields that changed
// between before and after, using server-side apply with the operator's field
// manager. A full status Update would race with the harness
// heartbeat/checkpoint writer and could overwrite fields the operator does
// not own.
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
	if before.Status.PR != after.Status.PR {
		fields.PR = after.Status.PR
	}
	if reflect.DeepEqual(fields, status.OperatorPatch{}) {
		return nil
	}
	writer := status.NewOperatorWriter(r.statusWriter())
	return writer.Patch(ctx, client.ObjectKeyFromObject(after), fields)
}

func (r *CoderRunReconciler) statusWriter() status.PatchWriter {
	if r.StatusWriter != nil {
		return r.StatusWriter
	}
	return status.KubePatchWriter{Client: r.Client}
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

func coordinatorExitCode(pod *corev1.Pod) (int32, bool) {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "coordinator" && status.State.Terminated != nil {
			return status.State.Terminated.ExitCode, true
		}
	}
	return 0, false
}

func terminalPhaseForExit(exitCode int32) courierv1alpha1.Phase {
	switch exitCode {
	case 0:
		return courierv1alpha1.PhaseAwaitingReview
	case 2:
		return courierv1alpha1.PhaseNeedsHuman
	default:
		return courierv1alpha1.PhaseFailed
	}
}
