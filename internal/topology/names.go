// Package topology renders the per-run secure topology (HARNESS.md §2-§3):
// one trusted control pod, one untrusted worker pod, one trusted broker pod
// behind a run-specific ClusterIP Service, and the per-run service accounts,
// RBAC, secrets, and network policies the operator owns and garbage-collects.
//
// Rendering is pure: every function takes its inputs and returns objects
// without touching the API. The launcher in internal/controller owns
// creation order, replacement serialization, and revocation.
package topology

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// LabelRun and LabelComponent mirror the executor's labels. The control
	// pod deliberately uses the same component value the broker's
	// authenticator expects for a control pod ("coordinator").
	LabelRun = "courier.misospace.dev/coderrun"

	ComponentCoordinator = "coordinator"
	ComponentBroker      = "broker"
	ComponentWorker      = "worker"

	labelAppManagedBy = "app.kubernetes.io/managed-by"
	labelAppComponent = "app.kubernetes.io/component"
)

// Resource names are derived from the run name with a fixed suffix. Long run
// names are truncated with a digest so every derived name stays a valid
// DNS-1123 subdomain and remains unique and stable per run. Objects with a
// stricter naming rule derive their names through podResourceName (the Pods,
// whose first label must fit the 63-character DNS-label bound a pod's
// hostname is drawn from) or serviceResourceName (the Services, which must be
// RFC-1035 DNS labels) instead.
func resourceName(runName, suffix string) string {
	base := strings.Trim(strings.ToLower(runName), "-")
	maxBase := 253 - len(suffix) - 9
	if len(base) > maxBase {
		digest := sha256.Sum256([]byte(runName))
		hash := hex.EncodeToString(digest[:])[:8]
		// Trailing dots are trimmed with the hyphens: a label may not begin
		// with a hyphen, and the digest is joined after this prefix.
		base = strings.TrimRight(base[:maxBase-len(hash)-1], "-.") + "-" + hash
	}
	return base + suffix
}

// podResourceName derives a Pod object name. Pods are validated as DNS-1123
// subdomains like every other object here, but a pod's hostname is the first
// label of its name truncated to 63 characters, and in-cluster consumers read
// that hostname, so the first label is held to the 63-character DNS-label
// bound for the two to agree. Names whose first label already fits pass
// through resourceName unchanged; an overflowing first label is truncated and
// digest-suffixed with a digest of the full run name, the way
// serviceResourceName bounds Service names, and the suffix keeps the
// per-component and probe tags distinct.
func podResourceName(runName, suffix string) string {
	const maxLabelLen = 63
	name := resourceName(runName, suffix)
	label := name
	if dot := strings.IndexByte(label, '.'); dot >= 0 {
		label = label[:dot]
	}
	if len(label) <= maxLabelLen {
		return name
	}
	digest := sha256.Sum256([]byte(runName))
	hash := hex.EncodeToString(digest[:])[:8]
	keep := maxLabelLen - len(suffix) - len(hash) - 1
	return strings.TrimRight(label[:keep], "-") + "-" + hash + suffix
}

// serviceResourceName derives a Service object name. Kubernetes validates
// Service names as RFC-1035 DNS labels: at most 63 characters, a lowercase
// letter first, only lowercase alphanumerics and hyphens, and an alphanumeric
// last. Run names are DNS subdomains, so they may contain dots or outgrow the
// label limit once the suffix is appended. A run name that is not already a
// valid label base within the limit maps deterministically to a normalized
// prefix plus an 8-hex digest of the full run name, so distinct run names
// stay distinct through normalization and truncation. Names that already fit
// pass through unchanged.
func serviceResourceName(runName, suffix string) string {
	const maxLabelLen = 63
	if isDNSLabelBase(runName) && len(runName)+len(suffix) <= maxLabelLen {
		return runName + suffix
	}
	digest := sha256.Sum256([]byte(runName))
	hash := hex.EncodeToString(digest[:])[:8]
	maxBase := maxLabelLen - len(suffix) - len(hash) - 1
	base := strings.ReplaceAll(strings.ToLower(runName), ".", "-")
	if base == "" || base[0] < 'a' || base[0] > 'z' {
		// Reserve one character for the letter prefix that anchors the label.
		maxBase--
	}
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	// Trailing dots are trimmed along with the hyphens before the digest is
	// joined, the way resourceName does it: normalization replaces dots with
	// hyphens first, but the cut prefix handed to the join is never allowed
	// to end in a dot, which would follow a run name's label with a hyphen.
	base = strings.Trim(base, "-.")
	if base == "" {
		base = "run"
	} else if base[0] < 'a' || base[0] > 'z' {
		base = "r" + base
	}
	return base + "-" + hash + suffix
}

// isDNSLabelBase reports whether s is a usable body of a DNS-1035 label:
// non-empty, lowercase alphanumeric and hyphen characters only, a lowercase
// letter first, and an alphanumeric last.
func isDNSLabelBase(s string) bool {
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
		default:
			return false
		}
	}
	return s[len(s)-1] != '-'
}

// RunLabelValue is the value the run label (LabelRun) is written with, so a
// reader selecting on it matches the topology objects the operator creates.
// All secure runs share one namespace, so the value must stay a correct run
// selector: it is sanitized to a label-safe form and, when that overflows the
// 63-character label cap, truncated and hash-suffixed with a digest of the
// full run name so two runs sharing a long prefix do not collide.
func RunLabelValue(runName string) string {
	sanitized := sanitizeLabelValue(runName)
	const max = 63
	if len(sanitized) <= max {
		return sanitized
	}
	digest := sha256.Sum256([]byte(runName))
	hash := hex.EncodeToString(digest[:])[:8]
	cut := max - len(hash) - 1
	return strings.TrimRight(sanitized[:cut], "-_.") + "-" + hash
}

// sanitizeLabelValue lowercases a value and replaces any character outside
// the allowed label set with a hyphen, trimming the ends. It returns
// "unknown" for an empty result.
func sanitizeLabelValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, char := range value {
		switch {
		case (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.':
			b.WriteRune(char)
		default:
			b.WriteByte('-')
		}
	}
	value = strings.Trim(b.String(), "-_.")
	if value == "" {
		return "unknown"
	}
	return value
}

// ControlPodName, BrokerPodName, WorkerPodName and the rest name every
// object the operator provisions for one run. Pod names go through
// podResourceName so the first label stays a valid DNS label; objects
// validated only as DNS subdomains stay on resourceName.
func ControlPodName(runName string) string { return podResourceName(runName, "-control") }

func BrokerPodName(runName string) string { return podResourceName(runName, "-broker") }

func WorkerPodName(runName string) string { return podResourceName(runName, "-worker") }

func ControlSAName(runName string) string { return resourceName(runName, "-control") }

func BrokerSAName(runName string) string { return resourceName(runName, "-broker") }

func WorkerSAName(runName string) string { return resourceName(runName, "-worker") }

func BrokerServiceName(runName string) string { return serviceResourceName(runName, "-broker") }

func WorkerServiceName(runName string) string { return serviceResourceName(runName, "-worker") }

func BrokerTLSSecretName(runName string) string { return resourceName(runName, "-broker-tls") }

func SigningSecretName(runName string) string { return resourceName(runName, "-control-signing") }

// GatewaySecretName is the per-run copy of the deployment's model-gateway
// key, created by the operator and mounted only into trusted control.
func GatewaySecretName(runName string) string { return resourceName(runName, "-gateway") }

func PolicySecretName(runName string) string { return resourceName(runName, "-broker-policy") }

func CredentialsSecretName(runName string) string { return resourceName(runName, "-provider-creds") }

func BrokerRoleName(runName string) string { return resourceName(runName, "-broker") }

func BrokerRoleBindingName(runName string) string { return resourceName(runName, "-broker") }

func BrokerTokenReviewBindingName(runName string) string {
	return resourceName(runName, "-broker-tokenreview")
}

func WorkerNetworkPolicyName(runName string) string { return resourceName(runName, "-worker") }

func ControlNetworkPolicyName(runName string) string { return resourceName(runName, "-control") }

func BrokerNetworkPolicyName(runName string) string { return resourceName(runName, "-broker") }

// BrokerServiceDNS returns the in-cluster DNS name of the run's broker
// Service, the only address control ever uses to reach the broker.
func BrokerServiceDNS(namespace, runName string) string {
	return BrokerServiceName(runName) + "." + namespace + ".svc.cluster.local"
}

// WorkerServiceDNS returns the in-cluster DNS name of the run's headless
// worker Service, the only address control ever uses to reach the worker.
func WorkerServiceDNS(namespace, runName string) string {
	return WorkerServiceName(runName) + "." + namespace + ".svc.cluster.local"
}

// Container names. The control pod's container is "coordinator" because the
// operator's observation and termination contract keys on that name; broker
// and worker containers are named after their components.
const (
	ControlContainerName = "coordinator"
	BrokerContainerName  = "broker"
	WorkerContainerName  = "worker"
)

// Runtime paths inside the pods.
const (
	// control
	controlSigningKeyPath = "/var/run/courier/signing/signing.key"
	controlBrokerCAPath   = "/var/run/courier/broker/ca.crt"
	controlTokenPath      = "/var/run/secrets/tokens/broker-token"
	controlRuntimePath    = "/courier-runtime"
	controlWorkspacePath  = "/workspace"
	// ControlWorkspacePath is the control pod's private integration tree.
	// It is never mounted into the worker.
	ControlWorkspacePath  = controlWorkspacePath
	controlGatewayKeyPath = "/var/run/courier/gateway/key"
	// broker
	brokerPolicyPath   = "/var/run/courier/broker/policy.json"
	brokerTLSCertPath  = "/var/run/courier/tls/tls.crt"
	brokerTLSKeyPath   = "/var/run/courier/tls/tls.key"
	brokerScratchPath  = "/var/tmp/courier-broker"
	brokerAPIAddr      = ":8443"
	brokerStatusAddr   = ":8444"
	brokerTokenPath    = "/var/run/secrets/tokens/api/token"
	brokerKubeCAPath   = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	brokerPortName     = "https"
	brokerStatusPortNm = "https-status"
	// worker
	workerWorkspacePath = "/workspace"

	// WorkerWorkspacePath is the untrusted worker's ephemeral workspace.
	// Trusted control composes the snapshot unpack and artifact pack task
	// scripts against it; it is never mounted into the control pod.
	WorkerWorkspacePath = workerWorkspacePath
	// WorkerPort is the task-listener port; keep in sync with the protocol
	// package's WorkerPort.
	WorkerPort     = 8080
	workerPortName = "tasks"
)

// Environment variable names carrying identity and configuration into the
// pods. Credential values travel only as SecretKeyRefs.
const (
	EnvPodNamespace = "POD_NAMESPACE"

	EnvRunUID = "COURIER_RUN_UID"
	// EnvControlPodUID carries the minted control-incarnation identifier that
	// the signed worker protocol binds envelopes to. It is not the pod's
	// Kubernetes UID.
	EnvControlPodUID = "COURIER_CONTROL_POD_UID"
	// EnvControlKubeUID carries the control pod's own Kubernetes UID via
	// downward API. It fences every trusted status write: the broker compares
	// it against the TokenReview's pod UID, and the operator compares it
	// against the live coordinator pod's UID.
	EnvControlKubeUID = "COURIER_CONTROL_POD_KUBE_UID"
	EnvWorkerPodUID   = "COURIER_WORKER_POD_UID"
	EnvWorkerPodName  = "COURIER_WORKER_POD_NAME"
	EnvBrokerPodName  = "COURIER_BROKER_POD_NAME"
	EnvBrokerPodUID   = "COURIER_BROKER_POD_UID"

	EnvWorkerPublicKey = "COURIER_WORKER_PUBLIC_KEY"
	EnvWorkerURL       = "COURIER_WORKER_URL"
	EnvBrokerURL       = "COURIER_BROKER_URL"
	EnvBrokerStatusURL = "COURIER_BROKER_STATUS_URL"
	EnvBrokerCAFile    = "COURIER_BROKER_CA"
	EnvSigningKeyFile  = "COURIER_SIGNING_KEY"
	EnvBrokerTokenFile = "COURIER_BROKER_TOKEN"

	// EnvGatewayURL and EnvGatewayKeyFile carry the deployment-level model
	// gateway configuration into trusted control (HARNESS.md §5). The key is
	// a mounted Secret value, never an environment value.
	EnvGatewayURL     = "COURIER_GATEWAY_URL"
	EnvGatewayKeyFile = "COURIER_GATEWAY_KEY_FILE"

	EnvControlSA    = "COURIER_CONTROL_SA"
	EnvControlSAUID = "COURIER_CONTROL_SA_UID"

	EnvProviderName     = "COURIER_FORGE_PROVIDER_NAME"
	EnvProviderType     = "COURIER_FORGE_PROVIDER_TYPE"
	EnvProviderEndpoint = "COURIER_FORGE_PROVIDER_ENDPOINT"
	EnvProviderGitEndp  = "COURIER_FORGE_PROVIDER_GIT_ENDPOINT"

	// EnvForgeAPIToken and the git credential pair are resolved from the
	// registration's credential references through per-run Secret copies.
	// The git pair names match the git broker's askpass contract.
	EnvForgeAPIToken = "COURIER_FORGE_API_TOKEN"
	EnvGitUsername   = "COURIER_GIT_USERNAME"
	EnvGitToken      = "COURIER_GIT_TOKEN"

	// EnvAPITokenFile points the broker at its projected API token.
	EnvAPITokenFile = "COURIER_API_TOKEN_FILE"

	// Dependency cache pins (worker only). The cache is reached as an IP
	// literal; the worker has no DNS.
	EnvGoProxy = "GOPROXY"
	EnvGoSumDB = "GOSUMDB"
)

// DefaultGitUsername is the git username used for token-based HTTPS pushes
// when a registration does not configure one explicitly.
const DefaultGitUsername = "x-access-token"

// Token constants.
const (
	BrokerAudience         = "courier-broker"
	ControlTokenSeconds    = 600
	FinalizerName          = "courier.misospace.dev/secure-topology"
	TokenReviewClusterRole = "courier-broker-tokenreview"
)
