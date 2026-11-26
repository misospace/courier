package executor

import (
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The enforced git write boundary for App-mode legacy runs (#902, #919).
//
// A git askpass helper can never bind a served token to the ref a push is
// about to update — git does not tell the helper — so the credential binding
// lives in a dedicated guard: since #919 a per-run guard POD (not a sidecar —
// a sidecar would share the coordinator pod's network namespace and force
// the forge CIDRs to stay fabric-admitted, the #908 residual), reached over
// a run-scoped Service whose DNS name the App-mode coordinator's git remotes
// are rewritten to. The guard injects the run's installation credentials
// itself and refuses every push that does not update exactly the run's
// approved work ref; the coordinator container carries no credential and no
// means to read one — its remotes name only the guard Service, and the
// projected token that reads the run's credentials Secret is mounted in the
// guard pod and in the coordinator pod's observer containers, never in the
// model container. The coordinator policy can therefore exclude the forge
// CIDRs outright (internal/topology's GitGuardEgressPolicy); a direct forge
// connection from the coordinator pod is refused by the fabric.
const (
	// GitEgressContainerName is the guard container's name, inside the guard
	// pod. The pod-level App-mode marker is the annotation below; the
	// container name is the executor binary's own contract.
	GitEgressContainerName = "git-egress"

	// GitGuardComponentName is the component label value that selects the
	// per-run guard pod (and only it) for policies, the guard Service, and
	// the terminal teardown.
	GitGuardComponentName = "git-guard"

	// GitEgressContractAnnotation marks a run's App-mode git credential
	// contract. Recognition is by presence; the version records the contract
	// revision the pod builders render — "2" is the dedicated guard pod, and
	// the version bump must never gate recognition: readers judge presence,
	// so pods rendered by an older contract keep working across an operator
	// upgrade.
	GitEgressContractAnnotation = "courier.misospace.dev/git-egress"
	GitEgressContractVersion    = "2"

	// GitEgressPort is the guard's fixed port: the guard pod listens on it
	// and the per-run Service forwards it. The port sits below the Linux
	// ephemeral range so no ordinary outbound socket can transiently squat
	// it inside the guard pod.
	GitEgressPort = 18443

	// GitGuardListenAddress is the address the guard pod's container binds:
	// every interface, because the Service forwards to the pod IP. Plain
	// HTTP here is intentional, as it was for the loopback sidecar: the
	// transport carries no credential — the guard injects the credential
	// into its own upstream TLS — and the guard's authority, not a TLS
	// chain, decides what the credential may do. What bounds who may dial
	// the guard is the guard pod's ingress NetworkPolicy (the run's own
	// pods), not the bind address; on a fabric that does not enforce
	// policies the whole boundary is advisory and the CIDR flag stays unset.
	GitGuardListenAddress = ":18443"

	// EnvGitEgressAddress names the guard's listen address so one producer
	// (the pod builder) decides it.
	EnvGitEgressAddress = "COURIER_GIT_EGRESS_ADDRESS"
)

// guardRemotePlaceholder marks where the repository owner/name travels in
// the GitRemoteURL template while it is parsed and rewritten.
const guardRemotePlaceholder = "courier-repo-placeholder"

// guardRemoteTemplate rewrites an https GitRemoteURL template to a
// guard-routed http origin — the per-run guard Service's DNS name and port
// in production, a stand-in host where only the template's shape is being
// validated — preserving the path shape (and so the repository path the
// guard pins). A template without the %s placeholder or with a non-https
// scheme is a configuration refusal: App mode cannot prove the guard fronts
// the real forge otherwise.
func guardRemoteTemplate(template, address string) (string, error) {
	if !strings.Contains(template, "%s") {
		return "", fmt.Errorf("executor: App-mode GitRemoteURL must carry the %%s repository placeholder: %q", template)
	}
	parsed, err := url.Parse(strings.Replace(template, "%s", guardRemotePlaceholder, 1))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Opaque != "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		!strings.Contains(parsed.EscapedPath(), guardRemotePlaceholder) {
		return "", fmt.Errorf("executor: App-mode GitRemoteURL must be an https URL whose path carries the %%s placeholder: %q", template)
	}
	parsed.Scheme = "http"
	parsed.Host = address
	return strings.Replace(parsed.String(), guardRemotePlaceholder, "%s", 1), nil
}

// configuredRemotes resolves the run's absolute forge remotes from the
// deployment template: the work remote follows the head repository, falling
// back to the base, and the base remote always names the base repository.
func configuredRemotes(config PodConfig, invocation Invocation) (string, string) {
	workRepository := invocation.HeadRepo
	if strings.TrimSpace(workRepository) == "" {
		workRepository = invocation.Repo
	}
	work := config.GitRemoteURL
	if strings.Contains(work, "%s") {
		work = strings.Replace(work, "%s", escapedRepositoryPath(workRepository), 1)
	}
	base := config.GitRemoteURL
	if strings.Contains(base, "%s") {
		base = strings.Replace(base, "%s", escapedRepositoryPath(invocation.Repo), 1)
	}
	return work, base
}

// gitEgressContainer renders the guard container only — the executor binary
// in git-egress mode with the run's pinned remotes, identities, approved
// work branch, and credential-read environment. BuildGitGuardPod wraps it in
// the dedicated pod; nothing else may embed it, because a container outside
// the guard pod would put the credential-serving listener back inside a pod
// whose policy must refuse the forge.
func gitEgressContainer(config PodConfig, invocation Invocation) corev1.Container {
	workRemote, baseRemote := configuredRemotes(config, invocation)
	restartAlways := corev1.ContainerRestartPolicyAlways
	return corev1.Container{
		Name:            GitEgressContainerName,
		Image:           config.Image,
		ImagePullPolicy: config.ImagePullPolicy,
		Command:         []string{config.BootstrapBinary},
		Args:            []string{"git-egress"},
		RestartPolicy:   &restartAlways,
		Env: []corev1.EnvVar{
			{Name: EnvGitEgressAddress, Value: GitGuardListenAddress},
			{Name: EnvInstallationOwner, Value: config.InstallationGit.Owner},
			{Name: EnvInstallationEndpoint, Value: config.InstallationGit.APIEndpoint},
			{Name: EnvInstallationSecret, Value: config.InstallationGit.CredentialsSecret},
			{Name: EnvAPITokenFile, Value: GitCredentialsTokenPath + "/token"},
			{Name: EnvAPICertFile, Value: GitCredentialsTokenPath + "/ca.crt"},
			{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
			{Name: "COURIER_REPO", Value: invocation.Repo},
			{Name: "COURIER_HEAD_REPO", Value: invocation.HeadRepo},
			{Name: "COURIER_REPO_URL", Value: workRemote},
			{Name: "COURIER_BASE_REPO_URL", Value: baseRemote},
			{Name: "COURIER_BRANCH", Value: invocation.Branch},
		},
		VolumeMounts: []corev1.VolumeMount{{
			Name: gitCredentialsTokenVolumeName, MountPath: GitCredentialsTokenPath, ReadOnly: true,
		}},
		// Fixed small Courier-internal sizing, never the lane's: the guard
		// streams git traffic and holds no workspace state.
		Resources: courierv1alpha1.DefaultObserverResources(),
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: boolPtr(false),
			ReadOnlyRootFilesystem:   boolPtr(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		// No readiness probe: a guard that never comes up surfaces as the
		// coordinator's git transport failing, which is the loud, correct signal.
	}
}
