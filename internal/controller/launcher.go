package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

var (
	ErrLauncherClientRequired = errors.New("coordinator launcher: client is required")
	// ErrEvidenceConfiguration is returned when evidence intake is
	// half-configured: the URL and the HMAC key must be set together, so a
	// partial setup fails closed instead of minting tokens the intake cannot
	// verify.
	ErrEvidenceConfiguration = errors.New("coordinator launcher: evidence intake requires both a service URL and an HMAC key")
	// ErrEvidenceRunUIDRequired is returned when evidence is configured but
	// the run has no UID: the run UID is part of the token's binding, and an
	// empty UID mints a token the intake can never verify from its live read
	// of the run.
	ErrEvidenceRunUIDRequired = errors.New("coordinator launcher: evidence tokens require the run UID")
	// ErrProvisioningInFlight is returned when Launch's create collided with
	// a pod carrying a DeletionTimestamp: the slot is held by a pod being
	// torn down, not a live coordinator, so the reconciler must not record
	// Running. The reconciler treats this as "stay Claimed, requeue", the
	// same shape as the secure topology's in-flight signal; the pod delete
	// event normally wakes the next reconcile, with a bounded requeue as
	// the backstop. A live existing pod still resolves to a successful,
	// idempotent Launch.
	ErrProvisioningInFlight = errors.New("coordinator launcher: provisioning in flight; existing pod is terminating")
)

// CoordinatorLauncher creates the ephemeral coordinator Pod for an admitted
// run. The runtime configuration is deployment-specific; the default is the
// temporary headless OpenCode shim.
type CoordinatorLauncher struct {
	Client   client.Client
	Scheme   *runtime.Scheme
	Pod      executor.PodConfig
	Executor executor.Executor
	// EvidenceURL is the intake endpoint handed to coordinator pods for
	// failure-evidence capture. Empty leaves capture disabled.
	EvidenceURL string
	// EvidenceKey is the HMAC key that signs the per-incarnation token
	// minted in Launch.
	EvidenceKey []byte
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
	// The intake URL is trimmed once so surrounding whitespace from a flag
	// value never reaches the pod: url.Parse rejects a leading space, so an
	// untrimmed value would dead-end capture with no operator-side signal.
	intakeURL := strings.TrimSpace(l.EvidenceURL)
	// A URL without a key (or a key without a URL) would hand the intake a
	// token it cannot route, so a half-configured launcher fails closed
	// before any API read.
	if (intakeURL != "" || l.EvidenceKey != nil) && (intakeURL == "" || len(l.EvidenceKey) == 0) {
		return ErrEvidenceConfiguration
	}
	// A malformed intake URL would only surface when the run's evidence is
	// POSTed, so reject it before any cluster read or pod creation.
	if intakeURL != "" {
		if err := executor.ValidateEvidenceURL(intakeURL); err != nil {
			return err
		}
	}
	var lane courierv1alpha1.LaneProfile
	if err := l.Client.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Lane}, &lane); err != nil {
		return err
	}
	// The nonce is minted here, per build, because the pod UID does not
	// exist until the API server assigns it after create: the intake needs a
	// per-incarnation secret before the pod exists, so it cannot be derived
	// from the pod itself.
	podConfig := l.Pod
	if intakeURL != "" || l.EvidenceKey != nil {
		// The run UID is part of the token's binding; an empty UID mints a
		// token the intake can never verify, so reject it before minting.
		if run.UID == "" {
			return ErrEvidenceRunUIDRequired
		}
		nonce, err := executor.NewEvidenceNonce()
		if err != nil {
			return err
		}
		podConfig.EvidenceURL = intakeURL
		podConfig.EvidenceToken = executor.EvidenceToken(l.EvidenceKey, run.Namespace, run.Name, string(run.UID), nonce)
		podConfig.EvidenceNonce = hex.EncodeToString(nonce)
	}
	builder := executor.NewPodBuilder(podConfig)
	builder.Executor = l.Executor
	pod, err := builder.Build(run, &lane)
	if err != nil {
		return err
	}
	err = l.Client.Create(ctx, pod)
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	// The create collided with an existing pod. A live one means Launch was
	// already idempotent at the resource boundary, so nothing more to do;
	// the reconciler's resumeClaimed path will see the live pod and finish
	// the admission without a second charge. A terminating one means the
	// slot is being torn down and writing Running over it would (a) bind
	// the run to a dying coordinator that the next reconcile could
	// terminalize as Failed, and (b) charge a second infrastructure loss
	// if the pod disappears before the wedge path observes it. Surface the
	// collision so the reconciler can stay Claimed and wait for the delete.
	var existing corev1.Pod
	if getErr := l.Client.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, &existing); getErr == nil && existing.DeletionTimestamp != nil {
		return ErrProvisioningInFlight
	}
	return nil
}
