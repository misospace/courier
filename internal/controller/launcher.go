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
	// ErrProvisioningInFlight is returned when Launch could not establish a
	// live, run-owned coordinator pod at the slot name. Three shapes fit
	// here, and each one means "stay Claimed, do not record Running":
	//
	//   - The create collided with a pod that is being torn down
	//     (DeletionTimestamp != nil). The slot is held by a pod whose
	//     disappearance is in flight.
	//   - The follow-up read of the existing pod failed (NotFound, a
	//     transient API error, or a Forbidden). The slot's live state
	//     could not be verified; recording Running would re-introduce the
	//     false-running transition #217 exists to prevent.
	//   - The follow-up read returned a pod that is not owned by this run
	//     (no Courier OwnerReference, no LabelRun). A pod sitting at the
	//     deterministic name but unrelated to this run must not be
	//     silently adopted as "idempotently live."
	//
	// The reconciler treats this as "stay Claimed, requeue", the same
	// shape as the secure topology's in-flight signal; the pod's delete
	// event normally wakes the next reconcile, with a bounded requeue as
	// the backstop. Only a follow-up read that returns a run-owned pod
	// not being torn down resolves the collision to a successful,
	// idempotent Launch; a completed-but-not-yet-deleted owned pod also
	// resolves it, and the observation path terminalizes it with a single
	// charge as before.
	ErrProvisioningInFlight = errors.New("coordinator launcher: provisioning in flight; slot could not be verified as a live run-owned pod")
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
	// The create collided with an existing pod. Three shapes are possible
	// and each one must surface as "stay Claimed, do not record Running":
	//
	//   - The follow-up Get fails (NotFound, transient, Forbidden). The
	//     slot's live state is unverified; treating that as success would
	//     re-introduce the false-running transition #217 exists to
	//     prevent, because the reconciler would then write Running over a
	//     slot we have no proof is occupied by a live coordinator.
	//   - The existing pod carries DeletionTimestamp: the slot is being
	//     torn down and the next reconcile could terminalize a freshly
	//     relaunched run as Failed off the dying pod's own termination,
	//     or charge a second infrastructure loss if the pod vanishes
	//     before the wedge path observes it.
	//   - The existing pod does not belong to this run (no Courier
	//     OwnerReference and no LabelRun). The deterministic slot name
	//     can collide with stale or unrelated state; silently adopting
	//     such a pod as "idempotently live" would bind the run to an
	//     arbitrary coordinator.
	//
	// A run-owned existing pod not being torn down is the only case that
	// resolves the collision to a successful, no-second-charge Launch: the
	// resource boundary was already idempotent and resumeClaimed will
	// finish the admission in its next pass.
	var existing corev1.Pod
	getErr := l.Client.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, &existing)
	switch {
	case getErr != nil:
		// Could not verify the slot's live state (NotFound is the
		// common race: the pod was deleted between the Create and the
		// follow-up Get; transient API errors fall here too). Stay
		// Claimed and let the bounded requeue / pod watch resolve it.
		return ErrProvisioningInFlight
	case existing.DeletionTimestamp != nil:
		return ErrProvisioningInFlight
	case !podBelongsToRun(&existing, run):
		return ErrProvisioningInFlight
	}
	return nil
}
