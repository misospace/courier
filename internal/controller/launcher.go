package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
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
	//   - The follow-up read returned a pod not owned by this run's UID
	//     (no controller Courier OwnerReference bound to the run's UID).
	//     A pod sitting at the deterministic name but tied to another
	//     run incarnation — or to no run at all — must not be silently
	//     adopted as "idempotently live."
	//
	// The follow-up read goes through the uncached API reader, not the
	// informer cache: a cached pod predating a deletion timestamp the API
	// server already carries must not resolve the collision to success.
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
	Client client.Client
	// APIReader reads the API server directly, bypassing the informer
	// cache. The AlreadyExists collision verification is a liveness check
	// that gates PhaseRunning, so it must see the API server's current
	// state: a cached pod predating a deletion timestamp would resolve a
	// terminating slot to a successful launch, the exact false-running
	// transition #217 exists to prevent. The manager wiring injects it;
	// when nil the collision read falls back to the cached client, which
	// serves only direct-construction tests.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Pod       executor.PodConfig
	Executor  executor.Executor
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
	criteria, err := criteriaEnvelope(run)
	if err != nil {
		return err
	}
	podConfig.Criteria = criteria
	builder := executor.NewPodBuilder(podConfig)
	builder.Executor = l.Executor
	pod, err := builder.Build(run, &lane)
	if err != nil {
		return err
	}
	// The isolated legacy observer's run-scoped identity — ServiceAccount,
	// Role, RoleBinding — is provisioned and ownership-verified BEFORE the
	// coordinator pod can begin execution (#327 single-writer guarantee).
	// The API server resolves the ServiceAccount at create time, so a
	// foreign same-named identity must never be admitted under, and Role or
	// RoleBinding provisioning must never fail only after a live writer
	// already exists. The same ensure is idempotent: retries and collision
	// adoption re-verify existing run-owned resources instead of adopting
	// anything unowned.
	if pod.Annotations[executor.ObserverContractAnnotation] == executor.ObserverContractVersion {
		// The run-scoped identity must be on the pod before it is created;
		// setting it afterwards would not persist, and the observer would
		// run as the namespace default SA with no access to its own pod.
		pod.Spec.ServiceAccountName = legacyObserverResourceName(run.Name)
		if err := l.ensureLegacyObserverResources(ctx, run, pod); err != nil {
			return l.provisioningFailure(ctx, pod, err)
		}
	}
	err = l.Client.Create(ctx, pod)
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		// A failed create is normally claim-safe, but an ambiguous failure
		// (a timeout after the API server persisted the pod, or a pod left
		// at the deterministic name by an earlier attempt of this still-
		// Claimed run) must not release the source claim while a writer may
		// occupy the slot.
		return l.provisioningFailure(ctx, pod, err)
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
	//   - The existing pod is not owned by this run's UID (no controller
	//     OwnerReference whose UID is the run's). The deterministic slot
	//     name can collide with stale or unrelated state — including a
	//     live coordinator left by a deleted-and-recreated CoderRun of
	//     the same name — and silently adopting such a pod as
	//     "idempotently live" would bind the run to an arbitrary
	//     coordinator.
	//
	// A run-owned existing pod not being torn down is the only case that
	// resolves the collision to a successful, no-second-charge Launch: the
	// resource boundary was already idempotent and resumeClaimed will
	// finish the admission in its next pass.
	var existing corev1.Pod
	getErr := l.collisionReader().Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, &existing)
	switch {
	case getErr != nil:
		// Could not verify the slot's live state (NotFound is the
		// common race: the pod was deleted between the Create and the
		// follow-up Get; transient API errors fall here too). Stay
		// Claimed and let the bounded requeue / pod watch resolve it.
		return ErrProvisioningInFlight
	case existing.DeletionTimestamp != nil:
		return ErrProvisioningInFlight
	case !podOwnedByRunUID(&existing, run.UID):
		return ErrProvisioningInFlight
	}
	// The observer resources were ensured and verified above, before the
	// create that produced this collision; the adopted run-owned slot is
	// fully provisioned.
	return nil
}

// provisioningFailure classifies a launch failure that happened while the
// coordinator slot's state is in question. Releasing the source claim
// resets the run to Pending and lets another capacity be handed the work —
// safe only while the slot is proven empty. If a pod exists at the slot
// (or its absence cannot be proven through the uncached reader), a writer
// may be live, so the failure surfaces as ErrProvisioningInFlight and the
// run stays Claimed instead of racing a live coordinator for ownership.
func (l *CoordinatorLauncher) provisioningFailure(ctx context.Context, pod *corev1.Pod, cause error) error {
	var existing corev1.Pod
	err := l.collisionReader().Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, &existing)
	if apierrors.IsNotFound(err) {
		return cause
	}
	return ErrProvisioningInFlight
}

// collisionReader returns the reader the AlreadyExists collision
// verification goes through, preferring the injected uncached API reader and
// falling back to the cached client. Production wiring always injects the
// API reader, so the fallback serves only direct-construction tests; the
// liveness decision that gates PhaseRunning never runs against cached state.
func (l *CoordinatorLauncher) collisionReader() client.Reader {
	if l.APIReader != nil {
		return l.APIReader
	}
	return l.Client
}

func (l *CoordinatorLauncher) ensureLegacyObserverResources(ctx context.Context, run *courierv1alpha1.CoderRun, pod *corev1.Pod) error {
	name := legacyObserverResourceName(run.Name)
	if err := l.ensureObserverObject(ctx, run.UID, l.observerServiceAccount(run)); err != nil {
		return err
	}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: name, OwnerReferences: l.observerOwner(run)}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, ResourceNames: []string{pod.Name}, Verbs: []string{"get"}}}}
	if err := l.ensureObserverObject(ctx, run.UID, role); err != nil {
		return err
	}
	return l.ensureObserverObject(ctx, run.UID, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: name, OwnerReferences: l.observerOwner(run)}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}, Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: name, Namespace: run.Namespace}}})
}

func (l *CoordinatorLauncher) observerOwner(run *courierv1alpha1.CoderRun) []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: courierv1alpha1.GroupVersion.String(), Kind: "CoderRun", Name: run.Name, UID: run.UID, Controller: boolPtr(true), BlockOwnerDeletion: boolPtr(true)}}
}

func (l *CoordinatorLauncher) observerServiceAccount(run *courierv1alpha1.CoderRun) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: legacyObserverResourceName(run.Name), OwnerReferences: l.observerOwner(run)}, AutomountServiceAccountToken: boolPtr(false)}
}

func (l *CoordinatorLauncher) ensureObserverObject(ctx context.Context, runUID types.UID, desired client.Object) error {
	if err := l.Client.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		// A same-named leftover from a deleted run must never be adopted:
		// garbage collection will remove it, and until then the launch
		// stays unverified.
		return l.verifyObserverResource(ctx, desired, runUID)
	}
	return nil
}

func (l *CoordinatorLauncher) verifyObserverResource(ctx context.Context, desired client.Object, runUID types.UID) error {
	key := client.ObjectKeyFromObject(desired)
	actual := desired.DeepCopyObject().(client.Object)
	// Ownership is judged through the uncached reader, like the pod-slot
	// gate: Create already hit the API server, so a cached Get could verify
	// a deleted-and-replaced same-named resource against a stale owned view
	// and admit the pod under a live foreign identity.
	if err := l.collisionReader().Get(ctx, key, actual); err != nil {
		return err
	}
	if !resourceOwnedByUID(actual, runUID) {
		return fmt.Errorf("legacy observer resource %s is not owned by run UID %q", key.Name, runUID)
	}
	switch want := desired.(type) {
	case *corev1.ServiceAccount:
		got := actual.(*corev1.ServiceAccount)
		if !reflect.DeepEqual(want.AutomountServiceAccountToken, got.AutomountServiceAccountToken) {
			return fmt.Errorf("legacy observer service account %s has unexpected spec", key.Name)
		}
	case *rbacv1.Role:
		if !reflect.DeepEqual(want.Rules, actual.(*rbacv1.Role).Rules) {
			return fmt.Errorf("legacy observer role %s has unexpected rules", key.Name)
		}
	case *rbacv1.RoleBinding:
		got := actual.(*rbacv1.RoleBinding)
		if !reflect.DeepEqual(want.RoleRef, got.RoleRef) || !reflect.DeepEqual(want.Subjects, got.Subjects) {
			return fmt.Errorf("legacy observer role binding %s has unexpected spec", key.Name)
		}
	}
	return nil
}

func resourceOwnedByUID(obj metav1.Object, uid types.UID) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == uid && ref.Kind == "CoderRun" && ref.Controller != nil && *ref.Controller {
			return true
		}
	}
	return false
}

func legacyObserverResourceName(runName string) string {
	return "courier-observer-" + executor.RunLabelValue(runName)
}

// criteriaEnvelope renders the trusted COURIER_CRITERIA envelope for one run
// from its persisted publication policy (#280): the pinned criterion set —
// re-verified by digest like every other policy field — serialized in the
// broker criterion-set shape. Empty means no set was pinned (fix-pr runs,
// policies admitted before the criterion contract, runs without a persisted
// policy). A corrupt pin fails closed: a run whose envelope cannot be
// trusted never launches with one.
func criteriaEnvelope(run *courierv1alpha1.CoderRun) (string, error) {
	policy := run.Status.PublicationPolicy
	if policy == nil {
		return "", nil
	}
	set, pinned, err := broker.CriterionSetFromPolicy(policy)
	if err != nil {
		return "", fmt.Errorf("launcher: pinned acceptance criteria are corrupt: %w", err)
	}
	if !pinned {
		return "", nil
	}
	encoded, err := json.Marshal(set)
	if err != nil {
		return "", fmt.Errorf("launcher: encode pinned acceptance criteria: %w", err)
	}
	return string(encoded), nil
}
