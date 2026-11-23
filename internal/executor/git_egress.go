package executor

import (
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The enforced git write boundary for App-mode legacy pods (#902).
//
// A git askpass helper can never bind a served token to the ref a push is
// about to update — git does not tell the helper — so the credential
// binding lives in a dedicated guard container instead: the coordinator pod
// runs courier-executor in git-egress mode as a loopback git smart-HTTP
// guard that injects the run's installation credentials itself and refuses
// every push that does not update exactly the run's approved work ref. The
// model container carries no credential and no means to read one: its git
// remotes are loopback URLs into the guard, and the projected token that
// reads the run's credentials Secret is mounted only in the guard container.
const (
	// GitEgressContainerName is the guard sidecar's container name. Its
	// presence (or the annotation below) marks a pod as App-mode.
	GitEgressContainerName = "git-egress"

	// GitEgressContractAnnotation marks a pod whose writes are bound by the
	// git egress guard. Recognition is by presence; the version records the
	// contract revision the pod builder renders.
	GitEgressContractAnnotation = "courier.misospace.dev/git-egress"
	GitEgressContractVersion    = "1"

	// GitEgressAddress is the pod-local loopback address the guard serves
	// and the App-mode coordinator's git remotes are rewritten to. Git over
	// plain http to a loopback address is intentional: the transport is
	// process-local, the guard terminates TLS to the real forge upstream,
	// and the guard's authority — not a TLS chain — decides what the
	// credential may do. The port sits below the Linux ephemeral range so
	// no ordinary outbound socket can transiently squat the guard's address.
	GitEgressAddress = "127.0.0.1:18443"

	// EnvGitEgressAddress names the guard's listen address so one producer
	// (the pod builder) decides it.
	EnvGitEgressAddress = "COURIER_GIT_EGRESS_ADDRESS"
)

// loopbackRemotePlaceholder marks where the repository owner/name travels in
// the GitRemoteURL template while it is parsed and rewritten.
const loopbackRemotePlaceholder = "courier-repo-placeholder"

// loopbackRemoteTemplate rewrites an https GitRemoteURL template to the
// guard's loopback origin, preserving the path shape (and so the repository
// path the guard pins). A template without the %s placeholder or with a
// non-https scheme is a configuration refusal: App mode cannot prove the
// guard fronts the real forge otherwise.
func loopbackRemoteTemplate(template, address string) (string, error) {
	if !strings.Contains(template, "%s") {
		return "", fmt.Errorf("executor: App-mode GitRemoteURL must carry the %%s repository placeholder: %q", template)
	}
	parsed, err := url.Parse(strings.Replace(template, "%s", loopbackRemotePlaceholder, 1))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Opaque != "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		!strings.Contains(parsed.EscapedPath(), loopbackRemotePlaceholder) {
		return "", fmt.Errorf("executor: App-mode GitRemoteURL must be an https URL whose path carries the %%s placeholder: %q", template)
	}
	parsed.Scheme = "http"
	parsed.Host = address
	return strings.Replace(parsed.String(), loopbackRemotePlaceholder, "%s", 1), nil
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

// gitEgressContainer renders the guard sidecar: the only container that
// mounts the projected token reading the run's credentials Secret, and the
// only process that can serve the run's git credential. Its environment is
// the same installation contract the pre-#902 coordinator container carried
// (owner, endpoint, Secret name, projected token), plus the run's pinned
// remotes, repository identities, and the approved work ref the guard
// enforces.
func gitEgressContainer(config PodConfig, invocation Invocation) corev1.Container {
	workRemote, baseRemote := configuredRemotes(config, invocation)
	return corev1.Container{
		Name:            GitEgressContainerName,
		Image:           config.Image,
		ImagePullPolicy: config.ImagePullPolicy,
		Command:         []string{config.BootstrapBinary},
		Args:            []string{"git-egress"},
		Env: []corev1.EnvVar{
			{Name: EnvGitEgressAddress, Value: GitEgressAddress},
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
		// No readiness probe: the guard listens on loopback only — a
		// pod-address listener would let any pod in the namespace borrow the
		// run's credential — and a kubelet probe cannot reach a loopback
		// bind. A guard that never comes up surfaces as the coordinator's
		// own git transport failing, which is the loud, correct signal.
	}
}
