package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// CoderRunReconciler reconciles a CoderRun object.
type CoderRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderuns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderuns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=coderuns/finalizers,verbs=update
// +kubebuilder:rbac:groups=courier.misospace.dev,resources=laneprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete

// Reconcile is the CoderRun control loop.
//
// This is a stub. The phase machine is specified in DESIGN.md ("Reconcile
// loop"): Pending -> Claimed -> Running -> AwaitingReview | NeedsHuman | Done |
// Failed, with concurrency admission per LaneProfile, source-side claim/
// transition, pod launch/resume, and heartbeat-based liveness. Each of those is
// tracked as a separate repo issue; this stub only proves the wiring.
func (r *CoderRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var run courierv1alpha1.CoderRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	l.V(1).Info("reconcile (stub)",
		"phase", run.Status.Phase,
		"mode", run.Spec.Mode,
		"repo", run.Spec.Repo,
		"ref", run.Spec.Ref,
		"lane", run.Spec.Lane,
	)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the controller with the manager.
func (r *CoderRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&courierv1alpha1.CoderRun{}).
		Complete(r)
}
