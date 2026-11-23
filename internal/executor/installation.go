package executor

import (
	"fmt"
	"net/url"
	"strings"
)

// App-mode git credential contract for the legacy coordinator pod (#871).
//
// When the deployment configures the GitHub App installation minter, the
// legacy launcher writes the run's installation-token document into the run's
// credentials Secret (the same document shape and Secret key the secure
// broker reads) and renders the coordinator pod with these variables instead
// of the static COURIER_GIT_USERNAME/COURIER_GIT_TOKEN pair. The pod's git
// credential helper (the executor binary's askpass mode) then resolves a
// short-lived, repository-pinned token per prompt and never consults a static
// credential: an App-enabled pod has no static fallback.
//
// The names are literals here because internal/executor cannot import
// internal/topology (topology imports executor); topology.CredentialsSecretName
// and topology.CredKeyInstallationToken are the parallel producers of the
// Secret name suffix and document key, and both sides are pinned by tests.
const (
	// EnvGitInstallation marks the pod as serving git credentials from the
	// installation-token document. The executor treats only the exact value
	// "1" as enabled; any other value (including a present-but-empty one) is
	// a broken App-mode configuration the helper refuses closed rather than
	// a silent return to static credentials.
	EnvGitInstallation = "COURIER_GIT_INSTALLATION"

	// EnvInstallationOwner is the installation owner the document must
	// carry. It is deployment configuration, never a secret.
	EnvInstallationOwner = "COURIER_INSTALLATION_OWNER"

	// EnvInstallationEndpoint is the forge API endpoint the document must
	// be bound to, compared with the same normalization the broker applies.
	EnvInstallationEndpoint = "COURIER_INSTALLATION_ENDPOINT"

	// EnvAPITokenFile names the file holding the pod's projected
	// ServiceAccount token for reading the credentials Secret. It shares the
	// broker's variable name so one contract describes both readers, and is
	// its single producer: topology.EnvAPITokenFile aliases this constant
	// (topology imports executor), so the two readers cannot drift.
	EnvAPITokenFile = "COURIER_API_TOKEN_FILE"

	// EnvAPICertFile optionally overrides the Kubernetes API server CA
	// bundle path. Absent, the helper uses DefaultAPICertPath, the
	// in-cluster ServiceAccount CA. The override exists for deterministic
	// fixtures; it carries no secret.
	EnvAPICertFile = "COURIER_API_CA_FILE"

	// EnvInstallationSecret names the run credentials Secret holding the
	// installation-token document. The launcher renders topology's resolved
	// name here rather than letting the pod derive it: the resolved name
	// truncates-and-digests long run names, and a pod-side derivation could
	// silently drift from it for exactly those names and fail closed forever.
	EnvInstallationSecret = "COURIER_INSTALLATION_SECRET"

	// InstallationTokenDocumentKey is the credentials Secret key carrying
	// the installation-token document. It mirrors
	// topology.CredKeyInstallationToken.
	InstallationTokenDocumentKey = "forge-installation-token"

	// DefaultAPICertPath is the in-cluster ServiceAccount CA bundle the
	// helper trusts when EnvAPICertFile is unset.
	DefaultAPICertPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

	// GitCredentialsTokenPath is the directory where an App-mode coordinator
	// pod mounts its projected ServiceAccount token for the credentials read;
	// COURIER_API_TOKEN_FILE names token <inside> it.
	GitCredentialsTokenPath = "/var/run/courier-git-credentials"
)

// InstallationGitConfig configures App-mode git credentials for the legacy
// coordinator pod. Nil means the deployment has not opted in: pods keep the
// static COURIER_GIT_* credential path byte-identically.
type InstallationGitConfig struct {
	// Owner is the installation owner every minted document is bound to.
	Owner string
	// APIEndpoint is the forge API endpoint minted documents are bound to,
	// the same identity endpoint the operator's minter signs for.
	APIEndpoint string
	// CredentialsSecret is the resolved run credentials Secret name
	// (topology.CredentialsSecretName) whose installation-token document
	// the pod's helper reads. The launcher resolves it because the name's
	// long-run truncation-and-digest rule belongs to exactly one producer.
	CredentialsSecret string
}

// Validate rejects a configuration that could not produce a document the
// pod's credential helper would accept: an empty owner, or an endpoint that
// is not an absolute https URL free of userinfo, query, and fragment.
func (c *InstallationGitConfig) Validate() error {
	if c == nil {
		return nil
	}
	if strings.TrimSpace(c.Owner) == "" || strings.TrimSpace(c.Owner) != c.Owner || strings.ContainsAny(c.Owner, "\x00\r\n") {
		return fmt.Errorf("executor: installation git owner is invalid: %q", c.Owner)
	}
	if strings.TrimSpace(c.CredentialsSecret) == "" || strings.TrimSpace(c.CredentialsSecret) != c.CredentialsSecret || strings.ContainsAny(c.CredentialsSecret, "\x00\r\n/") {
		return fmt.Errorf("executor: installation git credentials Secret name is invalid: %q", c.CredentialsSecret)
	}
	endpoint, err := url.Parse(strings.TrimSpace(c.APIEndpoint))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return fmt.Errorf("executor: installation git API endpoint must be an absolute https URL without userinfo, query, or fragment: %q", c.APIEndpoint)
	}
	return nil
}
