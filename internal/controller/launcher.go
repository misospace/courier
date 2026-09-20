package controller

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

var ErrLauncherClientRequired = errors.New("coordinator launcher: client is required")

// CoordinatorLauncher creates the ephemeral coordinator Pod for an admitted
// run. The runtime configuration is deployment-specific; the default is the
// temporary headless OpenCode shim.
type CoordinatorLauncher struct {
	Client   client.Client
	Scheme   *runtime.Scheme
	Pod      executor.PodConfig
	Executor executor.Executor
}

// Launch implements LaunchFunc. The run is already Claimed when this method
// is called, so a failed create is returned to the reconciler, which releases
// the claim back to Pending. Phase transitions are the reconciler's to write
// through the status contract; the launcher only creates the pod.
func (l *CoordinatorLauncher) Launch(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	if l == nil || l.Client == nil {
		return ErrLauncherClientRequired
	}
	if run == nil {
		return executor.ErrNilRun
	}
	var lane courierv1alpha1.LaneProfile
	if err := l.Client.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, &lane); err != nil {
		return err
	}
	builder := executor.NewPodBuilder(l.Pod)
	builder.Executor = l.Executor
	pod, err := builder.Build(run, &lane)
	if err != nil {
		return err
	}
	if err := l.Client.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
