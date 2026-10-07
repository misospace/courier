package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

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
	if err := l.Client.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
