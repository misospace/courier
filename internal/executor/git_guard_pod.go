package executor

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// BuildGitGuardPod renders the run's dedicated git-guard pod (#919): the
// per-run process that serves the App-mode git credential and binds every
// write to the approved work ref, reached by the coordinator over the run's
// guard Service. It replaces the #902 sidecar so the coordinator pod's
// egress policy can refuse the forge outright — a sidecar shares the
// coordinator pod's network namespace, and a policy selects pods, not
// containers, so the forge CIDRs would have to stay admitted for the
// sidecar's own upstream TLS (#908's residual). The render is pure: the
// launcher provisions and owns the pod (create-or-verify, never adopt a
// foreign same-named object) and garbage collection removes it with the
// run. The container contract is byte-for-byte the sidecar's — same image
// and bootstrap binary in git-egress mode, same installation environment
// with the absolute forge remotes the guard pins, same read-only,
// capability-dropped, non-root context, same fixed sizing — except the
// listen address, which moves from loopback to every interface so the
// Service can forward to it; the guard pod's ingress policy bounds the
// client set the old loopback bind bounded. The pod carries no workspace,
// no evidence contract, and no credential material of its own: its only
// mount is the projected token that reads the run's credentials Secret,
// under the run-scoped identity the launcher provisions.
func BuildGitGuardPod(run *courierv1alpha1.CoderRun, lane *courierv1alpha1.LaneProfile, config PodConfig) (*corev1.Pod, error) {
	if run == nil {
		return nil, ErrNilRun
	}
	if lane == nil {
		return nil, ErrNilLane
	}
	if config.InstallationGit == nil {
		return nil, fmt.Errorf("executor: git guard requires installation git configuration")
	}
	if strings.TrimSpace(config.WorkspacePath) == "" {
		config.WorkspacePath = DefaultPodConfig().WorkspacePath
	}
	if runtimeImage := strings.TrimSpace(lane.Spec.RuntimeImage); runtimeImage != "" {
		config.Image = runtimeImage
	}
	if strings.TrimSpace(config.Image) == "" {
		config.Image = DefaultPodConfig().Image
	}
	invocation, err := NewInvocationWithContributionAndRebaseAssist(run, lane, config.WorkspacePath, config.Contribution, config.RebaseModelAssist)
	if err != nil {
		return nil, err
	}
	invocation.Criteria = config.Criteria
	if config.ImagePullPolicy == "" {
		config.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if strings.TrimSpace(config.BootstrapBinary) == "" {
		config.BootstrapBinary = DefaultPodConfig().BootstrapBinary
	}
	name := config.GitGuardServiceAccountName
	if name == "" {
		name = GitCredentialsServiceAccountName(run.Name)
	}
	guardContainer := gitEgressContainer(config, invocation)
	guardContainer.RestartPolicy = nil
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: GitGuardPodName(run.Name), Namespace: run.Namespace,
			Labels: map[string]string{
				LabelRun: RunLabelValue(run.Name), LabelComponent: GitGuardComponentName,
				"app.kubernetes.io/name": "courier", "app.kubernetes.io/component": GitGuardComponentName,
				"app.kubernetes.io/managed-by": "courier",
			},
			Annotations:     map[string]string{GitEgressContractAnnotation: GitEgressContractVersion},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: courierv1alpha1.GroupVersion.String(), Kind: "CoderRun", Name: run.Name, UID: run.UID, Controller: &controller, BlockOwnerDeletion: &controller}},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: name, AutomountServiceAccountToken: boolPtr(false), RestartPolicy: corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: boolPtr(true), RunAsUser: int64Ptr(CoordinatorUID), RunAsGroup: int64Ptr(CoordinatorGID), FSGroup: int64Ptr(CoordinatorGID), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Volumes:         []corev1.Volume{gitCredentialsTokenVolume()},
			Containers:      []corev1.Container{guardContainer},
		},
	}, nil
}
