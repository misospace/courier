package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/topology"
)

// Requeue delay while waiting for probe pods or terminating topology pods.
const secureRequeueDelay = 2 * time.Second

// +kubebuilder:rbac:groups="",resources=secrets,verbs=create;delete;get;list
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=create;delete;get;list
// +kubebuilder:rbac:groups="",resources=services,verbs=create;delete;get;list
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=create;delete;get;list
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=create;delete;get;list
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=create;delete;get;list
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=create;delete;get;list
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=selfsubjectaccessreviews,verbs=create

// LaunchSecure provisions or reconciles one run's secure topology and drives
// it toward a fully running control pod. It is idempotent and resumes from
// live object state, so a crash mid-provisioning is repaired by the next
// reconcile. A returned requeue means provisioning is still in flight; the
// run stays Claimed until the topology is complete.
func (s *SecureControl) LaunchSecure(ctx context.Context, run *courier.CoderRun) (ctrl.Result, error) {
	policy, err := VerifyPersistedPolicy(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !run.DeletionTimestamp.IsZero() {
		// A deleting run never starts a replacement.
		return ctrl.Result{}, nil
	}

	relaunch := s.topologyPodsExist(ctx, run)

	if err := s.provisionStatic(ctx, run, policy); err != nil {
		return ctrl.Result{}, err
	}
	if err := s.ensureFinalizer(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if err := s.Preflight(ctx, run, policy, relaunch); err != nil {
		return ctrl.Result{}, err
	}

	// Broker: ensure Service and pod. A failed broker is replaced in two
	// passes — Service and pod removed first, both recreated only once the
	// old pod is confirmed gone.
	brokerPod, err := s.pod(ctx, run, topology.ComponentBroker)
	if err != nil {
		return ctrl.Result{}, err
	}
	if brokerPod != nil {
		if brokerPod.DeletionTimestamp != nil {
			return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
		}
		if brokerPod.Status.Phase == corev1.PodFailed {
			if err := s.Client.Delete(ctx, brokerPod); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			service := topology.BrokerService(run)
			if err := s.Client.Delete(ctx, service); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
		}
	} else {
		if err := s.ensureObject(ctx, topology.BrokerService(run)); err != nil {
			return ctrl.Result{}, err
		}
		registration, err := s.selectedRegistration(run)
		if err != nil {
			return ctrl.Result{}, err
		}
		controlSA, err := s.serviceAccount(ctx, run, topology.ControlSAName(run.Name))
		if err != nil {
			return ctrl.Result{}, err
		}
		pod, err := topology.BrokerPod(run, s.Config.HarnessImage, registration.Projection(), string(controlSA.UID))
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := s.Client.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("secure topology: create broker pod: %w", err)
		}
	}

	// Worker and control are provisioned as one incarnation round. The worker
	// carries the round's incarnation identifier and public key; control
	// carries the worker's live pod UID. Replacing either side rotates the
	// signing key and re-provisions both, so envelopes signed by a previous
	// incarnation are rejected by construction (§3).
	workerPod, err := s.pod(ctx, run, topology.ComponentWorker)
	if err != nil {
		return ctrl.Result{}, err
	}
	if workerPod != nil && workerPod.DeletionTimestamp != nil {
		return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
	}
	controlPod, err := s.pod(ctx, run, topology.ComponentCoordinator)
	if err != nil {
		return ctrl.Result{}, err
	}
	if controlPod != nil && controlPod.DeletionTimestamp != nil {
		return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
	}
	workerCreated := false
	if workerPod == nil {
		if controlPod != nil {
			// A control pod without its paired worker is a broken round:
			// fence control so both are recreated together.
			if err := s.Client.Delete(ctx, controlPod); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
		}
		if err := s.rotateSigningKey(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		incarnation, err := mintIncarnation()
		if err != nil {
			return ctrl.Result{}, err
		}
		publicKey, err := s.signingPublicKey(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		cache, err := s.cachePin(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		workerPod, err = topology.WorkerPod(run, s.Config.HarnessImage, incarnation, publicKey, cache)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := s.Client.Create(ctx, workerPod); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("secure topology: create worker pod: %w", err)
		}
		// Re-read for the server-assigned UID.
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: workerPod.Namespace, Name: workerPod.Name}, workerPod); err != nil {
			return ctrl.Result{}, err
		}
		workerCreated = true
	}
	if controlPod == nil {
		if !workerCreated {
			// The surviving worker belongs to a previous control incarnation.
			// Replacing control rotates the key and re-provisions the round;
			// Fence tears down the surviving worker and the credentialed
			// broker so the next LaunchSecure pass lands a clean round
			// instead of reusing stale material.
			if err := s.Fence(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
		}
		controlSA, err := s.serviceAccount(ctx, run, topology.ControlSAName(run.Name))
		if err != nil {
			return ctrl.Result{}, err
		}
		incarnation := workerEnvValue(workerPod, topology.EnvControlPodUID)
		if incarnation == "" {
			// The freshly minted worker was provisioned without the
			// incarnation binding the control pod would replay; fence the
			// surviving round and requeue so a new worker lands from
			// scratch on the next LaunchSecure pass.
			if err := s.Fence(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: secureRequeueDelay}, nil
		}
		invocation, err := s.controlInvocation(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		gateway, err := s.gatewayConfig(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		pod, err := topology.ControlPod(run, topology.ControlInputs{
			Image:                 s.Config.HarnessImage,
			Run:                   invocation,
			GatewayURL:            gateway.url,
			GatewayKeyMounted:     gateway.keyMounted,
			ControlSAUID:          string(controlSA.UID),
			WorkerPodUID:          string(workerPod.UID),
			ControlIncarnationUID: incarnation,
			WorkerURL:             "http://" + topology.WorkerServiceDNS(run.Namespace, run.Name) + ":" + fmt.Sprint(topology.WorkerPort),
			BrokerURL:             "https://" + topology.BrokerServiceDNS(run.Namespace, run.Name) + ":8443",
			BrokerStatusURL:       "https://" + topology.BrokerServiceDNS(run.Namespace, run.Name) + ":8444",
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := s.Client.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("secure topology: create control pod: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

// controlInvocation assembles the run context the control pod carries: the
// same executor.Invocation a process executor would receive, built from the
// immutable spec, the current status, and the lane's roles and framing.
func (s *SecureControl) controlInvocation(ctx context.Context, run *courier.CoderRun) (executor.Invocation, error) {
	lane := &courier.LaneProfile{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Lane}, lane); err != nil {
		return executor.Invocation{}, fmt.Errorf("secure topology: read lane profile %q: %w", run.Spec.Lane, err)
	}
	return executor.NewInvocation(run, lane, topology.ControlWorkspacePath)
}

type gatewayConfig struct {
	url        string
	keyMounted bool
}

// gatewayConfig resolves the deployment-level model-gateway configuration
// for one run's control pod. The URL is configuration; the key travels as a
// per-run Secret copy the operator creates from the deployment's key Secret,
// touching the value only to copy it between Secrets.
func (s *SecureControl) gatewayConfig(ctx context.Context, run *courier.CoderRun) (gatewayConfig, error) {
	out := gatewayConfig{url: s.Config.GatewayURL}
	if out.url == "" {
		return out, nil
	}
	if strings.TrimSpace(s.Config.GatewayKeySecret) != "" {
		if err := s.ensureGatewayKeyCopy(ctx, run); err != nil {
			return out, err
		}
		out.keyMounted = true
	}
	return out, nil
}

// ensureGatewayKeyCopy copies the deployment's model-gateway key into the
// per-run Secret only trusted control mounts. Values move between Secrets
// and are never logged or exposed.
func (s *SecureControl) ensureGatewayKeyCopy(ctx context.Context, run *courier.CoderRun) error {
	existing := &corev1.Secret{}
	err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.GatewaySecretName(run.Name)}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("secure topology: read gateway key copy: %w", err)
	}
	parts := strings.SplitN(s.Config.GatewayKeySecret, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("secure topology: --model-gateway-key-secret must be namespace/name, got %q", s.Config.GatewayKeySecret)
	}
	keyName := s.Config.GatewayKeyName
	if keyName == "" {
		keyName = "key"
	}
	source := &corev1.Secret{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: parts[0], Name: parts[1]}, source); err != nil {
		return fmt.Errorf("secure topology: read gateway key Secret %s: %w", s.Config.GatewayKeySecret, err)
	}
	value, ok := source.Data[keyName]
	if !ok || len(value) == 0 {
		return fmt.Errorf("gateway key Secret %s has no %q key", s.Config.GatewayKeySecret, keyName)
	}
	secret, err := topology.GatewaySecret(run, value)
	if err != nil {
		return err
	}
	if err := s.Client.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("secure topology: create gateway key copy: %w", err)
	}
	return nil
}

// ObserveTopology applies the secure topology's operator-owned observation:
// a terminated control fences its worker; a dead worker or broker is
// infrastructure failure that fences the round and counts against the
// crashloop ceiling. The returned handled value reports whether the caller
// must stop further observation.
func (s *SecureControl) ObserveTopology(ctx context.Context, run *courier.CoderRun, pods []corev1.Pod) (ctrl.Result, bool, error) {
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, false, nil
	}
	var control, worker, broker *corev1.Pod
	for i := range pods {
		if pods[i].Labels[topology.LabelProbe] == "true" {
			continue
		}
		switch pods[i].Labels["courier.misospace.dev/component"] {
		case topology.ComponentCoordinator:
			control = &pods[i]
		case topology.ComponentWorker:
			worker = &pods[i]
		case topology.ComponentBroker:
			broker = &pods[i]
		}
	}
	if control == nil {
		// No control pod: provisioning is still in flight (the launch path
		// reconciles it) or the control is missing, which the disappearance
		// backstop confirms against the API server. Either way the launch
		// and liveness paths own the decision, not this observation.
		return ctrl.Result{}, false, nil
	}
	if controlTerminated(control) {
		// The control incarnation is over: fence its worker so no untrusted
		// executor outlives its supervisor, and take the broker's ingress and
		// pod down with it — the broker holds the run's copied credentials and
		// terminal runs are never reaped. Run-only material is removed by the
		// ordered finalizer when the CoderRun is deleted. The caller proceeds
		// to the exit-code mapping, which decides the run's phase. The worker
		// or broker may already be gone in the same observation (the guard
		// above requires only the control pod) — an absent pod needs no
		// fencing, which keeps this branch nil-safe.
		if worker != nil && worker.DeletionTimestamp == nil {
			if err := s.Client.Delete(ctx, worker); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, true, err
			}
		}
		if err := s.Client.Delete(ctx, topology.BrokerService(run)); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, true, err
		}
		if broker != nil && broker.DeletionTimestamp == nil {
			if err := s.Client.Delete(ctx, broker); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, true, err
			}
		}
		return ctrl.Result{}, false, nil
	}
	if control.DeletionTimestamp != nil {
		return ctrl.Result{}, false, nil
	}
	// With a live control, a worker or broker that is gone or being removed
	// is infrastructure loss the operator owns (§6): the worker death ends
	// the run's operations, the control cannot work without its worker or
	// publish without its broker, and recovery happens independently of any
	// heartbeat. The round is fenced and relaunched against the crashloop
	// ceiling exactly like a broken pod. This is only reachable after
	// provisioning: ObserveTopology runs for Running runs, and the phase
	// reaches Running only after the launch path has created all three pods,
	// so a missing component here is a post-launch vanishing, not a window
	// the launch path is still filling.
	if worker == nil || broker == nil || worker.DeletionTimestamp != nil || broker.DeletionTimestamp != nil {
		return s.fenceAndRelaunch(ctx, run)
	}
	if topologyBroken(worker) || topologyBroken(broker) {
		return s.fenceAndRelaunch(ctx, run)
	}
	return ctrl.Result{}, false, nil
}

// controlTerminated reports whether the control pod's coordinator container
// has exited.
func controlTerminated(control *corev1.Pod) bool {
	for _, cs := range control.Status.ContainerStatuses {
		if cs.Name == topology.ControlContainerName && cs.State.Terminated != nil {
			return true
		}
	}
	return false
}

// topologyBroken reports a pod that has definitively failed.
func topologyBroken(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false // terminating pods are not re-deleted; deletion is in flight
	}
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

// fenceAndRelaunch deletes the run's topology pods and returns the run to
// Claimed for relaunch, honoring the crashloop ceiling exactly like liveness.
func (s *SecureControl) fenceAndRelaunch(ctx context.Context, run *courier.CoderRun) (ctrl.Result, bool, error) {
	// Fence leaves the control pod in place (see its doc); relaunch needs
	// every component torn down so the next LaunchSecure pass produces a
	// fresh round with a freshly minted signing key.
	controlPod, err := s.pod(ctx, run, topology.ComponentCoordinator)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if controlPod != nil {
		if err := s.Client.Delete(ctx, controlPod); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, true, err
		}
	}
	if err := s.Fence(ctx, run); err != nil {
		return ctrl.Result{}, true, err
	}
	if run.Status.Restarts >= defaultMaxRestarts {
		result, err := s.transitionNeedsHuman(ctx, run, "secure topology failed repeatedly and reached the crashloop ceiling")
		return result, true, err
	}
	before := run.DeepCopy()
	run.Status.Restarts++
	run.Status.Phase = courier.PhaseClaimed
	if err := s.Patcher.patchStatus(ctx, before, run); err != nil {
		return ctrl.Result{}, true, err
	}
	return ctrl.Result{Requeue: true}, true, nil
}

// Fence tears the worker and the credentialed broker of the round down:
// the worker pod is deleted, the broker's ingress Service is deleted
// before the broker pod (HARNESS.md §3), and the broker pod is deleted.
// The control pod is intentionally left in place — at the ceiling the
// inert coordinator object is the only record of the cause the
// `NeedsHuman` hand-off keeps (#106), and below the ceiling the relaunch's
// `LaunchSecure` reproduces a fresh control from the persisted policy.
// Callers that are about to re-provision a fresh control pod must delete
// it themselves before invoking Fence (matching `ObserveTopology`'s
// control-terminated branch, which fences the worker and the broker but
// leaves the control pod as the run's last record). It is idempotent: a
// missing or already-deleting object is a no-op.
//
// This is the primitive the operator reaches for whenever the run's
// supervisor is gone or terminal — including the control-loss backstop
// (#215), where the detection reconcile must not leave the untrusted
// executor and the credentialed broker alive just because a later relaunch
// reconcile may never run. Terminal runs are never reaped, so a fence
// deferred past the detection reconcile never runs at all.
func (s *SecureControl) Fence(ctx context.Context, run *courier.CoderRun) error {
	workerPod, err := s.pod(ctx, run, topology.ComponentWorker)
	if err != nil {
		return err
	}
	if workerPod != nil {
		if err := s.Client.Delete(ctx, workerPod); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	// Disable broker ingress before the broker pod: the Service stops
	// routing before the pod stops serving, mirroring the ordering in
	// Revoke and in ObserveTopology's control-terminated branch.
	if err := s.Client.Delete(ctx, topology.BrokerService(run)); client.IgnoreNotFound(err) != nil {
		return err
	}
	brokerPod, err := s.pod(ctx, run, topology.ComponentBroker)
	if err != nil {
		return err
	}
	if brokerPod != nil {
		if err := s.Client.Delete(ctx, brokerPod); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// Revoke performs the ordered, idempotent revocation sequence for a deleting
// run (HARNESS.md §3): stop worker and control, disable broker ingress and
// delete the broker, then remove run-only keys, RBAC, services, and
// policies. It returns done=true when every object is gone and the finalizer
// may be removed. Each step treats an already-removed object as done, so a
// rerun after a partial failure resumes from live state and never
// re-provisions a grant.
func (s *SecureControl) Revoke(ctx context.Context, run *courier.CoderRun) (bool, error) {
	for _, component := range []string{topology.ComponentCoordinator, topology.ComponentWorker} {
		pod, err := s.pod(ctx, run, component)
		if err != nil {
			return false, err
		}
		if pod == nil {
			continue
		}
		if pod.DeletionTimestamp == nil {
			if err := s.Client.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
				return false, err
			}
		}
		return false, nil
	}
	brokerPod, err := s.pod(ctx, run, topology.ComponentBroker)
	if err != nil {
		return false, err
	}
	// Disable broker ingress first: the Service stops routing before the pod
	// stops serving and before any run-only material is removed. The Service
	// is deleted on every pass until it is confirmed gone, whether or not the
	// broker pod survives — a partial pass must never report done with broker
	// ingress still routed.
	service := &corev1.Service{}
	err = s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerServiceName(run.Name)}, service)
	serviceDeleted := false
	switch {
	case err == nil:
		if err := s.Client.Delete(ctx, topology.BrokerService(run)); client.IgnoreNotFound(err) != nil {
			return false, err
		}
		serviceDeleted = true
	case apierrors.IsNotFound(err):
		// Already gone: ingress is disabled.
	default:
		return false, err
	}
	if brokerPod != nil {
		if brokerPod.DeletionTimestamp == nil {
			if err := s.Client.Delete(ctx, brokerPod); client.IgnoreNotFound(err) != nil {
				return false, err
			}
		}
		return false, nil
	}
	if serviceDeleted {
		// Ingress is disabled but its removal is not yet confirmed; the next
		// pass resumes from live object state before run-only material is
		// removed.
		return false, nil
	}

	runName := run.Name
	namespace := run.Namespace
	objects := []client.Object{
		secretObject(namespace, topology.BrokerTLSSecretName(runName)),
		secretObject(namespace, topology.SigningSecretName(runName)),
		secretObject(namespace, topology.PolicySecretName(runName)),
		secretObject(namespace, topology.CredentialsSecretName(runName)),
		secretObject(namespace, topology.GatewaySecretName(runName)),
		topology.WorkerService(run),
		saObject(namespace, topology.ControlSAName(runName)),
		saObject(namespace, topology.BrokerSAName(runName)),
		saObject(namespace, topology.WorkerSAName(runName)),
		topology.BrokerRole(run),
		topology.BrokerRoleBinding(run),
		topology.BrokerTokenReviewBinding(run),
	}
	for _, object := range objects {
		if err := s.Client.Delete(ctx, object); client.IgnoreNotFound(err) != nil {
			return false, err
		}
	}
	for _, policy := range topology.NetworkPolicies(run, nil, nil) {
		if err := s.Client.Delete(ctx, policy); client.IgnoreNotFound(err) != nil {
			return false, err
		}
	}
	return true, nil
}

// RemoveFinalizer strips the secure-topology finalizer once revocation is
// complete.
func (s *SecureControl) RemoveFinalizer(ctx context.Context, run *courier.CoderRun) error {
	if !controllerutil.ContainsFinalizer(run, topology.FinalizerName) {
		return nil
	}
	base := run.DeepCopy()
	controllerutil.RemoveFinalizer(run, topology.FinalizerName)
	return s.Client.Patch(ctx, run, client.MergeFrom(base))
}

func (s *SecureControl) ensureFinalizer(ctx context.Context, run *courier.CoderRun) error {
	if controllerutil.ContainsFinalizer(run, topology.FinalizerName) {
		return nil
	}
	base := run.DeepCopy()
	controllerutil.AddFinalizer(run, topology.FinalizerName)
	return s.Client.Patch(ctx, run, client.MergeFrom(base))
}

// provisionStatic creates every non-workload object of the topology. Each
// create tolerates AlreadyExists; nothing here ever mutates an existing
// object, so replacement rounds cannot weaken a provisioned boundary.
func (s *SecureControl) provisionStatic(ctx context.Context, run *courier.CoderRun, policy *courier.PublicationPolicy) error {
	for _, sa := range topology.ServiceAccounts(run) {
		if err := s.ensureObject(ctx, sa); err != nil {
			return err
		}
	}
	if err := s.ensureObject(ctx, topology.BrokerRole(run)); err != nil {
		return err
	}
	if err := s.ensureObject(ctx, topology.BrokerRoleBinding(run)); err != nil {
		return err
	}
	if err := s.ensureObject(ctx, topology.BrokerTokenReviewBinding(run)); err != nil {
		return err
	}
	for _, policy := range topology.NetworkPolicies(run, s.cachePointer(ctx), s.gatewayPointer()) {
		if err := s.ensureObject(ctx, policy); err != nil {
			return err
		}
	}
	if err := s.ensureBrokerTLS(ctx, run); err != nil {
		return err
	}
	if err := s.ensureCredentialsCopy(ctx, run); err != nil {
		return err
	}
	if err := s.ensurePolicySecret(ctx, run, policy); err != nil {
		return err
	}
	if err := s.ensureObject(ctx, topology.BrokerService(run)); err != nil {
		return err
	}
	if err := s.ensureObject(ctx, topology.WorkerService(run)); err != nil {
		return err
	}
	return nil
}

func (s *SecureControl) cachePointer(ctx context.Context) *topology.CachePin {
	pin, err := s.cachePin(ctx)
	if err != nil {
		return nil
	}
	return pin
}

func (s *SecureControl) gatewayPointer() *topology.GatewayPin {
	if s.Config.GatewayCIDR == "" || s.Config.GatewayPort == 0 {
		return nil
	}
	return &topology.GatewayPin{CIDR: s.Config.GatewayCIDR, Port: s.Config.GatewayPort}
}

func (s *SecureControl) ensureBrokerTLS(ctx context.Context, run *courier.CoderRun) error {
	existing := &corev1.Secret{}
	err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.BrokerTLSSecretName(run.Name)}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("secure topology: read broker TLS secret: %w", err)
	}
	dnsNames := []string{
		topology.BrokerServiceDNS(run.Namespace, run.Name),
		topology.BrokerServiceName(run.Name) + "." + run.Namespace + ".svc",
		topology.BrokerServiceName(run.Name),
	}
	tls, err := topology.GenerateBrokerTLS(dnsNames)
	if err != nil {
		return err
	}
	secret, err := topology.BrokerTLSSecret(run, tls)
	if err != nil {
		return err
	}
	if err := s.Client.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("secure topology: create broker TLS secret: %w", err)
	}
	return nil
}

// ensureCredentialsCopy resolves the selected registration's credential
// references from the deployment's Secrets and copies the referenced values
// into the per-run Secret the broker pod reads. The operator touches the
// values only to copy them between Secrets and never logs or exposes them.
func (s *SecureControl) ensureCredentialsCopy(ctx context.Context, run *courier.CoderRun) error {
	existing := &corev1.Secret{}
	err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("secure topology: read credential copy: %w", err)
	}
	registration, err := s.selectedRegistration(run)
	if err != nil {
		return err
	}
	copyData, err := s.copyCredentials(ctx, registration)
	if err != nil {
		return err
	}
	secret, err := topology.CredentialsSecret(run, copyData.forgeAPIToken, copyData.gitUsername, copyData.gitToken)
	if err != nil {
		return err
	}
	if err := s.Client.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("secure topology: create credential copy: %w", err)
	}
	return nil
}

type credentialCopy struct {
	forgeAPIToken string
	gitUsername   string
	gitToken      string
}

func (s *SecureControl) copyCredentials(ctx context.Context, registration *forge.Registration) (credentialCopy, error) {
	out := credentialCopy{gitUsername: registration.GitUsername}
	if out.gitUsername == "" {
		out.gitUsername = topology.DefaultGitUsername
	}
	read := func(purpose string) (string, error) {
		ref, ok := registration.CredentialRef(purpose)
		if !ok {
			return "", fmt.Errorf("registration %q carries no %s credential reference", registration.Name, purpose)
		}
		secret := &corev1.Secret{}
		key := types.NamespacedName{Namespace: s.Config.OperatorNamespace, Name: ref.SecretName}
		if err := s.Client.Get(ctx, key, secret); err != nil {
			return "", fmt.Errorf("read credential Secret %s for provider %q: %w", ref.SecretName, registration.Name, err)
		}
		value, ok := secret.Data[ref.Key]
		if !ok || len(value) == 0 {
			return "", fmt.Errorf("credential Secret %s has no %q key", ref.SecretName, ref.Key)
		}
		return string(value), nil
	}
	var err error
	if out.forgeAPIToken, err = read(forge.PurposeForgeAPI); err != nil {
		return out, err
	}
	if out.gitToken, err = read(forge.PurposeGit); err != nil {
		return out, err
	}
	return out, nil
}

func (s *SecureControl) ensurePolicySecret(ctx context.Context, run *courier.CoderRun, policy *courier.PublicationPolicy) error {
	registration, err := s.selectedRegistration(run)
	if err != nil {
		return err
	}
	document := topology.BrokerPolicyDocument{PublicationPolicy: policy, Provider: registration.Projection()}
	rendered, err := topology.PolicySecret(run, document)
	if err != nil {
		return err
	}
	existing := &corev1.Secret{}
	err = s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: rendered.Name}, existing)
	if err == nil {
		if string(existing.Data["policy.json"]) != string(rendered.Data["policy.json"]) {
			return secureNeedsHuman("PolicyConflict",
				"the persisted broker policy secret does not match the resolved policy; refusing to serve a mutated policy")
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("secure topology: read policy secret: %w", err)
	}
	if err := s.Client.Create(ctx, rendered); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("secure topology: create policy secret: %w", err)
	}
	return nil
}

// rotateSigningKey replaces the per-control-incarnation signing key. It runs
// only when a new worker (and therefore a new control incarnation) is being
// provisioned.
func (s *SecureControl) rotateSigningKey(ctx context.Context, run *courier.CoderRun) error {
	old := &corev1.Secret{}
	old.Name = topology.SigningSecretName(run.Name)
	old.Namespace = run.Namespace
	if err := s.Client.Delete(ctx, old); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("secure topology: delete stale signing key: %w", err)
	}
	public, private, err := mintSigningKey()
	if err != nil {
		return err
	}
	secret, err := topology.SigningSecret(run, private)
	if err != nil {
		return err
	}
	if err := s.Client.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("secure topology: create signing key: %w", err)
	}
	_ = public
	return nil
}

func (s *SecureControl) signingPublicKey(ctx context.Context, run *courier.CoderRun) ([]byte, error) {
	secret := &corev1.Secret{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.SigningSecretName(run.Name)}, secret); err != nil {
		return nil, fmt.Errorf("secure topology: read signing key: %w", err)
	}
	raw, ok := secret.Data["signing.key"]
	if !ok || len(raw) != 64 {
		return nil, errors.New("secure topology: signing key is unusable")
	}
	return raw[32:], nil
}

// selectedRegistration re-selects the registration the persisted policy pins.
// The digest comparison guarantees selection and enforcement stay the same
// record across restarts and replacements.
func (s *SecureControl) selectedRegistration(run *courier.CoderRun) (*forge.Registration, error) {
	policy, err := VerifyPersistedPolicy(run)
	if err != nil {
		return nil, err
	}
	registration, err := s.Config.Registry.Select(run.Spec.Repo)
	if err != nil {
		return nil, secureNeedsHuman("ProviderSelection", "provider selection failed: %v", err)
	}
	if registration.Name != policy.ProviderConfigRef {
		return nil, secureNeedsHuman("ProviderSelection",
			"selected registration %q does not match the persisted policy's provider %q",
			registration.Name, policy.ProviderConfigRef)
	}
	projection := registration.Projection()
	if projection.Digest != policy.CredentialRefDigest {
		return nil, secureNeedsHuman("ProviderSelection",
			"registration %q projection digest does not match the persisted policy; the registry changed since admission",
			registration.Name)
	}
	return registration, nil
}

func (s *SecureControl) ensureObject(ctx context.Context, object client.Object) error {
	if err := s.Client.Create(ctx, object); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("secure topology: create %T %s: %w", object, object.GetName(), err)
	}
	return nil
}

// pod returns the run's live pod for one component, or nil when absent.
// Preflight probe pods carry the worker's component label (so the worker's
// own NetworkPolicy applies to them) and are excluded by the probe label.
func (s *SecureControl) pod(ctx context.Context, run *courier.CoderRun, component string) (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := s.Client.List(ctx, &pods, client.InNamespace(run.Namespace)); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels[topology.LabelProbe] == "true" {
			continue
		}
		if pod.Labels[topology.LabelRun] == run.Name && pod.Labels["courier.misospace.dev/component"] == component {
			return pod, nil
		}
	}
	return nil, nil
}

func (s *SecureControl) topologyPodsExist(ctx context.Context, run *courier.CoderRun) bool {
	for _, component := range []string{topology.ComponentCoordinator, topology.ComponentWorker, topology.ComponentBroker} {
		pod, err := s.pod(ctx, run, component)
		if err == nil && pod != nil {
			return true
		}
	}
	return false
}

func (s *SecureControl) serviceAccount(ctx context.Context, run *courier.CoderRun, name string) (*corev1.ServiceAccount, error) {
	sa := &corev1.ServiceAccount{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, sa); err != nil {
		return nil, fmt.Errorf("secure topology: read service account %s: %w", name, err)
	}
	return sa, nil
}

// workerEnvValue reads one env value from the pod's single container. Used
// only for values the operator itself provisioned (the incarnation binding).
func workerEnvValue(pod *corev1.Pod, name string) string {
	if pod == nil {
		return ""
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

// transitionNeedsHuman records the SecurePreflightFailed condition and
// terminalizes the run NeedsHuman. It lives on SecureControl but writes
// through the reconciler's own status path, so it is defined in
// coderun_controller.go's package with a narrow writer interface.
type statusPatcher interface {
	patchStatus(ctx context.Context, before, after *courier.CoderRun) error
	transitionTerminal(ctx context.Context, run *courier.CoderRun, phase courier.Phase, pr string, intent terminalLifecycleIntent, telemetry *courier.RunTelemetry) (ctrl.Result, error)
	emitPhaseTransition(run *courier.CoderRun, phase courier.Phase, detail map[string]any)
}

func (s *SecureControl) transitionNeedsHuman(ctx context.Context, run *courier.CoderRun, detail string) (ctrl.Result, error) {
	if s.Patcher == nil {
		return ctrl.Result{}, errors.New("secure topology: no reconciler wired for terminalization")
	}
	patcher := s.Patcher
	condition := metav1.Condition{
		Type:               "SecurePreflightFailed",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: run.Generation,
		LastTransitionTime: metav1.NewTime(s.now().UTC()),
		Reason:             "TopologyFailed",
		Message:            detail,
	}
	replaced := false
	for i := range run.Status.Conditions {
		if run.Status.Conditions[i].Type == "SecurePreflightFailed" {
			run.Status.Conditions[i] = condition
			replaced = true
			break
		}
	}
	if !replaced {
		run.Status.Conditions = append(run.Status.Conditions, condition)
	}
	return patcher.transitionTerminal(ctx, run, courier.PhaseNeedsHuman, "", terminalLifecycleIntent{error: detail}, nil)
}

func secretObject(namespace, name string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

func saObject(namespace, name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}
