package executor

import (
	corev1 "k8s.io/api/core/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

const ObserverContractAnnotation = "courier.misospace.dev/observer-contract"

// ObserverEvidence is the trusted, run-scoped result emitted by the isolated
// legacy workspace observer.
type ObserverEvidence struct {
	Version           int                      `json:"version"`
	RunUID            string                   `json:"run_uid"`
	PodUID            string                   `json:"pod_uid"`
	Branch            string                   `json:"branch"`
	Baseline          string                   `json:"baseline"`
	WriterContainerID string                   `json:"writer_container_id"`
	Safety            *WorkspaceSafetyEvidence `json:"safety,omitempty"`
}

const (
	observerStateVolume = "observer-state"
	observerTokenVolume = "observer-token"
	observerCAName      = "kube-root-ca.crt"
)

func observerVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name:         observerStateVolume,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name: observerTokenVolume,
			VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{
					{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: int64Ptr(3600)}},
					{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: observerCAName}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
				},
			}},
		},
	}
}

func observerSeedContainer(config PodConfig, invocation Invocation, podName, runUID string) corev1.Container {
	mounts := []corev1.VolumeMount{{Name: observerStateVolume, MountPath: ObserverStatePath}}
	// App-mode seeding fetches the trusted remotes to pin the baseline, so
	// the seed resolves installation credentials through the same projected
	// token volume the observer container uses (#871). Static seeds never
	// mount it.
	if config.InstallationGit != nil {
		mounts = append(mounts, corev1.VolumeMount{Name: observerTokenVolume, MountPath: ObserverTokenPath, ReadOnly: true})
	}
	return corev1.Container{
		Name: "workspace-observer-seed", Image: config.Image, ImagePullPolicy: config.ImagePullPolicy,
		Command: []string{config.BootstrapBinary}, Args: []string{"observe-seed"},
		Env:                      observerEnvironment(config, invocation, podName, runUID, false),
		TerminationMessagePath:   ObserverStatePath + "/seed-termination",
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		VolumeMounts:             mounts,
		// Fixed small Courier-internal sizing, never the lane's: see
		// courierv1alpha1.DefaultObserverResources. An unbounded seed could
		// otherwise consume the node past every sized container's limit.
		Resources:       courierv1alpha1.DefaultObserverResources(),
		SecurityContext: observerSecurityContext(false),
	}
}

func observerContainer(config PodConfig, invocation Invocation, podName, runUID string) corev1.Container {
	return corev1.Container{
		Name: ObserverContainerName, Image: config.Image, ImagePullPolicy: config.ImagePullPolicy,
		Command: []string{config.BootstrapBinary}, Args: []string{"observe-workspace"},
		Env:                      observerEnvironment(config, invocation, podName, runUID, true),
		TerminationMessagePath:   ObserverStatePath + "/termination",
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		VolumeMounts:             []corev1.VolumeMount{{Name: observerStateVolume, MountPath: ObserverStatePath}, {Name: "workspace", MountPath: config.WorkspacePath, ReadOnly: true}, {Name: observerTokenVolume, MountPath: ObserverTokenPath, ReadOnly: true}},
		// Fixed small Courier-internal sizing, never the lane's: see
		// courierv1alpha1.DefaultObserverResources.
		Resources:       courierv1alpha1.DefaultObserverResources(),
		SecurityContext: observerSecurityContext(true),
	}
}

func observerEnvironment(config PodConfig, invocation Invocation, podName, runUID string, observer bool) []corev1.EnvVar {
	var remote, baseRemote string
	for _, env := range podEnvironment(invocation, "opencode", config) {
		if env.Name == "COURIER_REPO_URL" {
			remote = env.Value
		}
		if env.Name == "COURIER_BASE_REPO_URL" {
			baseRemote = env.Value
		}
		if remote != "" && baseRemote != "" {
			break
		}
	}
	envs := []corev1.EnvVar{
		{Name: "COURIER_REPO_URL", Value: remote},
		{Name: "COURIER_BASE_REPO_URL", Value: baseRemote},
		{Name: "COURIER_BRANCH", Value: invocation.Branch},
		{Name: "COURIER_BASE", Value: config.BaseBranch},
		{Name: "COURIER_RUN_UID", Value: runUID},
		{Name: "COURIER_WORKSPACE", Value: config.WorkspacePath},
		{Name: "COURIER_BASELINE_FILE", Value: ObserverStatePath + "/private/baseline"},
		{Name: "COURIER_TERMINATION_FILE", Value: ObserverStatePath + "/termination"},
		{Name: "POD_NAME", Value: podName},
		{Name: "POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
	}
	for _, ref := range CredentialEnvRefs(config, invocation.Mode) {
		if ref.Name == "COURIER_GIT_USERNAME" || ref.Name == "COURIER_GIT_TOKEN" {
			envs = append(envs, corev1.EnvVar{Name: ref.Name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: ref.Secret}, Key: ref.Key}}})
		}
	}
	// App-mode git credentials (#871): the observer resolves the same
	// run document through its own projected identity (the token volume
	// it already mounts), bound to exactly the two configured remotes.
	// The installation variables replace the static git pair; the seed
	// gets the same repository identities and resolved Secret name the
	// coordinator carries, because the binding is built from the remote
	// URLs plus the pinned owner/name identities — without the head
	// repository a fork run would serve the base token against the fork.
	if config.InstallationGit != nil {
		envs = append(envs,
			corev1.EnvVar{Name: EnvGitInstallation, Value: "1"},
			corev1.EnvVar{Name: EnvInstallationOwner, Value: config.InstallationGit.Owner},
			corev1.EnvVar{Name: EnvInstallationEndpoint, Value: config.InstallationGit.APIEndpoint},
			corev1.EnvVar{Name: EnvInstallationSecret, Value: config.InstallationGit.CredentialsSecret},
			corev1.EnvVar{Name: "COURIER_REPO", Value: invocation.Repo},
			corev1.EnvVar{Name: "COURIER_HEAD_REPO", Value: invocation.HeadRepo},
		)
	}
	if observer {
		envs = append(envs, corev1.EnvVar{Name: "COURIER_TOKEN_FILE", Value: ObserverTokenPath + "/token"}, corev1.EnvVar{Name: "COURIER_CA_FILE", Value: ObserverTokenPath + "/ca.crt"})
	}
	return envs
}

func observerSecurityContext(readOnly bool) *corev1.SecurityContext {
	return &corev1.SecurityContext{RunAsNonRoot: boolPtr(true), RunAsUser: int64Ptr(ObserverUID), RunAsGroup: int64Ptr(ObserverUID), ReadOnlyRootFilesystem: boolPtr(readOnly), AllowPrivilegeEscalation: boolPtr(false), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
}
