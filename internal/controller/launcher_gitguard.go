package controller

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

func (l *CoordinatorLauncher) ensureGitGuardService(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	desired := topology.GitGuardService(run)
	if err := l.Client.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		actual := &corev1.Service{}
		if err := l.collisionReader().Get(ctx, client.ObjectKeyFromObject(desired), actual); err != nil {
			return err
		}
		if !resourceOwnedByUID(actual, run.UID) {
			return secureNeedsHuman("GitGuardServiceOwnership", "Service %q is not owned by this run", desired.Name)
		}
		updated := actual.DeepCopy()
		clusterIP, clusterIPs, ipFamilies, ipFamilyPolicy := updated.Spec.ClusterIP, updated.Spec.ClusterIPs, updated.Spec.IPFamilies, updated.Spec.IPFamilyPolicy
		updated.Spec = *desired.Spec.DeepCopy()
		updated.Spec.ClusterIP, updated.Spec.ClusterIPs, updated.Spec.IPFamilies, updated.Spec.IPFamilyPolicy = clusterIP, clusterIPs, ipFamilies, ipFamilyPolicy
		if !apiequality.Semantic.DeepEqual(updated.Spec, actual.Spec) {
			if err := l.Client.Update(ctx, updated); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *CoordinatorLauncher) requireGitGuardRunning(ctx context.Context, run *courierv1alpha1.CoderRun, name string) error {
	pod := &corev1.Pod{}
	if err := l.collisionReader().Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: name}, pod); err != nil {
		return ErrProvisioningInFlight
	}
	if !podOwnedByRunUID(pod, run.UID) || pod.DeletionTimestamp != nil {
		return ErrProvisioningInFlight
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == executor.GitEgressContainerName && status.State.Running != nil {
			return nil
		}
	}
	// A guard that never runs wedges the claim, visibly as the crashing guard
	// pod; without this gate, its first git failure terminalizes the run. The
	// wedge is preferable because that guard breaks every launch on the deployment.
	return ErrProvisioningInFlight
}

func (l *CoordinatorLauncher) ensureGitGuardPod(ctx context.Context, run *courierv1alpha1.CoderRun, desired *corev1.Pod) error {
	if err := l.Client.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		actual := &corev1.Pod{}
		if err := l.collisionReader().Get(ctx, client.ObjectKeyFromObject(desired), actual); err != nil {
			return ErrProvisioningInFlight
		}
		if actual.DeletionTimestamp != nil {
			return ErrProvisioningInFlight
		}
		if !podOwnedByRunUID(actual, run.UID) {
			return secureNeedsHuman("GitGuardPodOwnership", "Pod %q is not owned by this run", desired.Name)
		}
	}
	return nil
}

// TeardownLegacyGitGuard requests deletion of run-owned guard resources, but an
// accepted Pod delete is not termination: the guard ingress policy stays in
// place until an authoritative read observes no pod carrying the guard's
// labels — the exact set the policy protects, so a terminating or
// same-label-named replacement keeps the boundary. UID preconditions prevent
// deleting replacements between read and delete. A terminal run can still
// carry a live or deliberately preserved coordinator, so the coordinator's
// forge-refusal policy is never deleted here: it remains owned by the run and
// garbage collection removes it with the run after retention.
func (l *CoordinatorLauncher) TeardownLegacyGitGuard(ctx context.Context, run *courierv1alpha1.CoderRun) error {
	if l == nil || l.Client == nil || run == nil {
		return errors.New("legacy git guard teardown requires a launcher client and run")
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name)}}
	for _, object := range []client.Object{pod, service} {
		if err := l.collisionReader().Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !resourceOwnedByUID(object.(metav1.Object), run.UID) {
			continue
		}
		uid := object.GetUID()
		if err := l.Client.Delete(ctx, object, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	// The absence gate is the policy's own blast radius, not the guard pod's
	// name: the ingress policy selects by run-and-component labels, so any
	// pod carrying them — terminating, foreign, or merely labeled like the
	// guard — keeps the boundary until an authoritative read sees none.
	pods := &corev1.PodList{}
	if err := l.collisionReader().List(ctx, pods,
		client.InNamespace(run.Namespace),
		client.MatchingLabels(map[string]string{
			topology.LabelRun:       topology.RunLabelValue(run.Name),
			executor.LabelComponent: executor.GitGuardComponentName,
		})); err != nil {
		return err
	}
	if len(pods.Items) > 0 {
		return ErrGuardTeardownPending
	}

	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: topology.GitGuardPodNetworkPolicyName(run.Name)}}
	if err := l.collisionReader().Get(ctx, client.ObjectKeyFromObject(policy), policy); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	if !resourceOwnedByUID(policy, run.UID) {
		return nil
	}
	uid := policy.UID
	if err := l.Client.Delete(ctx, policy, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
