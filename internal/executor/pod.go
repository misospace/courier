package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

const (
	// CoordinatorUID and CoordinatorGID are the non-root identity contract
	// shared by the published coordinator image and its generated Pods.
	CoordinatorUID int64 = 65532
	CoordinatorGID int64 = 65532

	// Legacy observer identity and protected paths are shared with the
	// coordinator-side evidence reader.
	ObserverContainerName         = "workspace-observer"
	ObserverUID             int64 = 65533
	ObserverStatePath             = "/courier-observer"
	ObserverTokenPath             = "/var/run/courier-observer-token"
	ObserverContractVersion       = "1"

	LabelRun               = "courier.misospace.dev/coderrun"
	LabelComponent         = "courier.misospace.dev/component"
	LabelExecutor          = "courier.misospace.dev/executor"
	LabelRepo              = "courier.misospace.dev/repo"
	LabelMode              = "courier.misospace.dev/mode"
	LabelLane              = "courier.misospace.dev/lane"
	LabelDebug             = "courier.misospace.dev/debug"
	runtimePath            = "/courier-runtime"
	scratchPath            = "/var/tmp/courier-scratch"
	ToolchainCachePath     = "/courier-toolchain-cache" // read-write; lane toolchain caches (GOMODCACHE etc.) live here, path pinned by the runtime image
	ToolchainReferencePath = "/courier-toolchain"       // same volume, read-only; the coordinator's inspection entry point
	// EnvtestStorePath is where the runtime image bakes the pre-materialized
	// envtest asset store (#797): the contract's bootstrap resolves envtest
	// assets from it under the worker's offline contract instead of
	// downloading from the internet. Images without the bake have no such
	// directory; consumers guard on its presence and fall back.
	EnvtestStorePath = "/usr/local/share/courier/envtest-store"
	// EnvtestStoreEnv carries EnvtestStorePath into the worker environment.
	EnvtestStoreEnv = "COURIER_ENVTEST_STORE"
)

// PodConfig controls runtime-specific details without putting provider or
// hardware assumptions into the API. Empty fields receive safe defaults.
type PodConfig struct {
	Image              string
	ImagePullPolicy    corev1.PullPolicy
	WorkspacePath      string
	ServiceAccountName string
	RuntimeClassName   string
	// GitRemoteURL may contain one %s placeholder for the run's owner/name.
	// It is deployment configuration, not a provider-specific assumption.
	GitRemoteURL           string
	BaseBranch             string
	GitCredentialSecret    string
	GitUsernameKey         string
	GitTokenKey            string
	GitHubCredentialSecret string
	GitHubTokenKey         string
	EnvironmentSecret      string
	// Git is the author/committer identity rendered into the generated
	// pod's explicit container environment (#317). Empty fields resolve
	// to the explicit default identity. Because explicit container env
	// beats the envFrom EnvironmentSecret, a configured identity always
	// wins over same-named keys in that Secret; the standalone executor
	// keeps the opposite order (its environment wins, then the default).
	Git             GitIdentity
	GitHubMCPURL    string
	Context7MCPURL  string
	MetricsMCPURL   string
	BootstrapBinary string
	OpenCode        OpenCode
	// EvidenceURL, EvidenceToken, and EvidenceNonce enable bounded
	// failure-evidence capture. Capture is all-or-nothing: all three must
	// be set together, and leaving them empty keeps the pod on the kubelet's
	// default termination grace period.
	EvidenceURL   string
	EvidenceToken string
	EvidenceNonce string
	// Criteria is the per-run trusted COURIER_CRITERIA envelope: the
	// operator-authored JSON of the acceptance-criterion set pinned at
	// admission (#280), rendered from the persisted publication policy.
	// Empty means no set is pinned (fix-pr runs, pre-contract policies) and
	// emits no environment variable.
	Criteria string
	// Contribution is the source-neutral execution input a contribute-mode
	// run renders into its coordinator goal (#624). The value is supplied by
	// the launch path from the run's own source adapter; the pod builder never
	// derives it and never learns a source's context key. Empty for
	// resolve-issue and fix-pr runs, which carry their whole goal in the spec.
	Contribution ContributionExecution
	// RebaseModelAssist enables model-assisted conflict resolution in
	// rebase-run coordinator pods (#732): the builder emits
	// COURIER_REBASE_MODEL_ASSIST=enabled only when this is true and the run
	// is a rebase run. Absence of
	// the variable is the meaningful fail-closed default — the executor
	// parses everything but an explicit affirmative as off — so a disabled
	// configuration or any non-rebase run emits neither the flag nor its
	// supporting variables.
	RebaseModelAssist bool
	// ModelGatewayURL is the OpenAI-compatible model gateway API root the
	// rebase assist resolves conflicts through, emitted as
	// COURIER_MODEL_GATEWAY_URL (trimmed) only when RebaseModelAssist is
	// enabled; a disabled configuration emits nothing.
	ModelGatewayURL string
	// ModelGatewayKeySecret names the Secret holding the model-gateway API
	// key, injected as the COURIER_MODEL_GATEWAY_API_KEY SecretKeyRef only
	// when RebaseModelAssist is enabled and this is set; empty means an
	// unauthenticated gateway. Absence when disabled is again the
	// fail-closed default.
	ModelGatewayKeySecret string
	// ModelGatewayKeyName is the key within ModelGatewayKeySecret; empty
	// defaults to "key".
	ModelGatewayKeyName string
	// InstallationGit opts the deployment into App-mode git credentials for
	// legacy coordinator pods (#871): the pod renders a git egress guard
	// pod that serves the run's installation-token document and binds every
	// write to the run's approved work ref (#902), the coordinator container's
	// git remotes are rewritten to the guard Service, and the launcher
	// provisions the run-scoped identity the guard uses to read the token. Nil
	// keeps the static pair exactly as before; non-nil suppresses the git pair even when
	// GitCredentialSecret is configured, because an App-enabled run must
	// not hold a static git credential to fall back to.
	InstallationGit            *InstallationGitConfig
	GitGuardServiceAccountName string
}

// DefaultPodConfig is suitable for the temporary OpenCode shim image. The
// image remains configurable because it is deployment-specific.
func DefaultPodConfig() PodConfig {
	return PodConfig{
		Image:           "ghcr.io/misospace/courier-opencode:latest",
		ImagePullPolicy: corev1.PullIfNotPresent,
		WorkspacePath:   "/workspace",
		GitRemoteURL:    "https://github.com/%s.git",
		BaseBranch:      "main",
		Git:             DefaultGitIdentity(),
		BootstrapBinary: "/usr/local/bin/courier-executor",
		OpenCode:        DefaultOpenCode(),
	}
}

// PodBuilder creates one ephemeral coordinator Pod per CoderRun.
type PodBuilder struct {
	Config   PodConfig
	Executor Executor
}

// NewPodBuilder returns a builder using the temporary OpenCode shim unless a
// different executor is supplied later.
func NewPodBuilder(config PodConfig) *PodBuilder {
	return &PodBuilder{Config: config}
}

// BuildCoordinatorPod creates a Pod with an EmptyDir workspace and no durable
// storage. The Pod is intentionally only a process host; status checkpointing
// and resumable orchestration belong to the future harness.
func BuildCoordinatorPod(run *courierv1alpha1.CoderRun, lane *courierv1alpha1.LaneProfile, config PodConfig) (*corev1.Pod, error) {
	builder := NewPodBuilder(config)
	return builder.Build(run, lane)
}

// Build constructs a coordinator Pod from one run and lane.
func (b *PodBuilder) Build(run *courierv1alpha1.CoderRun, lane *courierv1alpha1.LaneProfile) (*corev1.Pod, error) {
	if run == nil {
		return nil, ErrNilRun
	}
	if lane == nil {
		return nil, ErrNilLane
	}
	config := b.Config
	if strings.TrimSpace(config.Image) == "" {
		config.Image = DefaultPodConfig().Image
	}
	if runtimeImage := strings.TrimSpace(lane.Spec.RuntimeImage); runtimeImage != "" {
		config.Image = runtimeImage
	}
	if config.ImagePullPolicy == "" {
		config.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if strings.TrimSpace(config.WorkspacePath) == "" {
		config.WorkspacePath = DefaultPodConfig().WorkspacePath
	}
	if strings.TrimSpace(config.BaseBranch) == "" {
		config.BaseBranch = DefaultPodConfig().BaseBranch
	}
	if strings.TrimSpace(config.BootstrapBinary) == "" {
		config.BootstrapBinary = DefaultPodConfig().BootstrapBinary
	}
	if strings.TrimSpace(config.ServiceAccountName) == "" {
		config.ServiceAccountName = DefaultPodConfig().ServiceAccountName
	}
	if config.OpenCode.Binary == "" && config.OpenCode.Format == "" {
		config.OpenCode = DefaultOpenCode()
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	executorImpl := b.Executor
	if executorImpl == nil {
		executorImpl = config.OpenCode
	}
	if strings.TrimSpace(executorImpl.Name()) == "" {
		return nil, ErrMissingCommandName
	}
	invocation, err := NewInvocationWithContributionAndRebaseAssist(run, lane, config.WorkspacePath, config.Contribution, config.RebaseModelAssist)
	if err != nil {
		return nil, err
	}
	// The trusted criterion envelope is operator-authored per run; the
	// executor contract only carries it, never derives it.
	invocation.Criteria = config.Criteria
	command := executorImpl.Command(invocation)
	if bootstrapper, ok := executorImpl.(Bootstrapper); ok {
		command = bootstrapper.BootstrapCommand(invocation, config.BootstrapBinary)
	}
	if strings.TrimSpace(command.Binary) == "" {
		return nil, ErrMissingCommandName
	}
	if strings.TrimSpace(run.Name) == "" {
		return nil, ErrMissingRunName
	}
	openCodeConfig, err := marshalOpenCodeConfig(invocation.Roles, config.GitHubMCPURL, config.Context7MCPURL, config.MetricsMCPURL)
	if err != nil {
		return nil, fmt.Errorf("executor: marshal OpenCode config: %w", err)
	}
	// App-mode coordinator containers talk to the per-run git egress Service,
	// never to the forge directly (#902). The guard pod and observer keep the
	// absolute forge remotes.
	coordinatorConfig := config
	if config.InstallationGit != nil {
		remote, err := guardRemoteTemplate(config.GitRemoteURL, GitGuardServiceDNS(run.Namespace, run.Name)+":"+strconv.Itoa(GitEgressPort))
		if err != nil {
			return nil, err
		}
		coordinatorConfig.GitRemoteURL = remote
	}

	labels := map[string]string{
		LabelRun:                       RunLabelValue(run.Name),
		LabelComponent:                 "coordinator",
		LabelExecutor:                  labelValue(executorImpl.Name()),
		LabelRepo:                      labelValue(run.Spec.Repo),
		LabelMode:                      labelValue(string(run.Spec.Mode)),
		LabelLane:                      labelValue(run.Spec.Lane),
		LabelDebug:                     fmt.Sprintf("%t", run.Spec.Debug),
		"app.kubernetes.io/name":       "courier",
		"app.kubernetes.io/component":  "coordinator",
		"app.kubernetes.io/managed-by": "courier",
	}
	runRef := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(run.Name),
			Namespace: run.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				opencodeConfigAnnotation: string(openCodeConfig),
			},

			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         courierv1alpha1.GroupVersion.String(),
				Kind:               "CoderRun",
				Name:               run.Name,
				UID:                run.UID,
				Controller:         &runRef,
				BlockOwnerDeletion: &runRef,
			}},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:           config.ServiceAccountName,
			AutomountServiceAccountToken: boolPtr(false),
			RuntimeClassName:             stringPtr(nonEmpty(config.RuntimeClassName)),
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   boolPtr(true),
				RunAsUser:      int64Ptr(CoordinatorUID),
				RunAsGroup:     int64Ptr(CoordinatorGID),
				FSGroup:        int64Ptr(CoordinatorGID),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Volumes: []corev1.Volume{{
				Name:         "workspace",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}, {
				Name:         "runtime",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}, {
				Name: "opencode-config",
				VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{{
					Path:     "opencode.json",
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + opencodeConfigAnnotation + "']"},
				}}}},
			}, {
				Name:         "scratch",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}, {
				Name:         "toolchain-cache",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
			Containers: []corev1.Container{{
				Name:                     "coordinator",
				Image:                    config.Image,
				ImagePullPolicy:          config.ImagePullPolicy,
				TerminationMessagePath:   runtimePath + "/termination",
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				Command:                  []string{command.Binary},
				Args:                     append([]string(nil), command.Args...),
				WorkingDir:               config.WorkspacePath,
				Env:                      podEnvironment(invocation, executorImpl.Name(), coordinatorConfig),
				Resources:                lane.Spec.EffectiveResources(),
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "workspace",
					MountPath: config.WorkspacePath,
				}, {
					Name:      "runtime",
					MountPath: runtimePath,
				}, {
					Name:      "opencode-config",
					MountPath: opencodeConfigMountPath,
					ReadOnly:  true,
				}, {
					Name:      "scratch",
					MountPath: scratchPath,
				}, {
					Name:      "toolchain-cache",
					MountPath: ToolchainCachePath,
				}, {
					Name:      "toolchain-cache",
					MountPath: ToolchainReferencePath,
					ReadOnly:  true,
				}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: boolPtr(false),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}
	if config.InstallationGit != nil {
		pod.Annotations[GitEgressContractAnnotation] = GitEgressContractVersion
	}
	// A 45-second grace only exists to fit the evidence capture deadline; a
	// run that does not capture keeps the kubelet's default.
	observerEnabled := executorImpl.Name() == "opencode"
	if observerEnabled {
		pod.Annotations[ObserverContractAnnotation] = ObserverContractVersion
		pod.Spec.Volumes = append(pod.Spec.Volumes, observerVolumes()...)
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, observerSeedContainer(coordinatorConfig, invocation, pod.Name, string(run.UID)))
		pod.Spec.Containers = append(pod.Spec.Containers, observerContainer(coordinatorConfig, invocation, pod.Name, string(run.UID)))
		// The pod-level fsGroup (65532) owns the shared emptyDir roots; the
		// observer's distinct UID/GID (65533) needs that group as a
		// supplement or it cannot create its private state directory. For
		// the coordinator it is a no-op (it already runs 65532:65532).
		pod.Spec.SecurityContext.SupplementalGroups = []int64{CoordinatorGID}
	}
	if config.EvidenceEnabled() {
		pod.Spec.TerminationGracePeriodSeconds = int64Ptr(EvidenceTerminationGracePeriodSeconds)
	}
	if strings.TrimSpace(config.EnvironmentSecret) != "" {
		pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: config.EnvironmentSecret}},
		}}
	}
	return pod, nil
}

// CredentialEnvRef is one per-key credential environment variable the Pod
// builder injects by SecretKeyRef.
type CredentialEnvRef struct {
	Name   string
	Secret string
	Key    string
}

// CredentialEnvRefs returns the (env name, Secret, key) triples the builder
// injects key-by-key. It is the single source of truth for what the executor
// receives and what the evidence intake (#200) re-scans against: one shared
// function produces the per-key credential references so the pod builder and
// the intake can never enumerate the same Secret keys differently. The
// envFrom environment Secret (--executor-environment-secret) is deliberately
// excluded: kubelet maps its keys 1:1 to env names the builder never
// enumerates, so the intake registers it by a different route. mode gates
// the mode-scoped entries: a credential only one execution mode uses never
// widens its exposure to pods of other modes.
func CredentialEnvRefs(config PodConfig, mode courierv1alpha1.Mode) []CredentialEnvRef {
	var refs []CredentialEnvRef
	// App-mode git credentials (#871) replace the static pair entirely: an
	// App-enabled run must not hold a static git credential it could fall
	// back to, so the pair is suppressed even when GitCredentialSecret is
	// configured. GITHUB_TOKEN keeps its fallback to the git Secret only in
	// static mode; in App mode a separately configured GitHubCredentialSecret
	// remains the deployment's explicit choice for the executor's forge API
	// reads, which are not the git transport this mode governs.
	installationMode := config.InstallationGit != nil
	if strings.TrimSpace(config.GitCredentialSecret) != "" && !installationMode {
		usernameKey := config.GitUsernameKey
		if usernameKey == "" {
			usernameKey = "username"
		}
		tokenKey := config.GitTokenKey
		if tokenKey == "" {
			tokenKey = "token"
		}
		refs = append(refs,
			CredentialEnvRef{Name: "COURIER_GIT_USERNAME", Secret: config.GitCredentialSecret, Key: usernameKey},
			CredentialEnvRef{Name: "COURIER_GIT_TOKEN", Secret: config.GitCredentialSecret, Key: tokenKey},
		)
	}
	githubSecret := config.GitHubCredentialSecret
	if githubSecret == "" && !installationMode {
		githubSecret = config.GitCredentialSecret
	}
	if strings.TrimSpace(githubSecret) != "" {
		githubTokenKey := config.GitHubTokenKey
		if githubTokenKey == "" {
			githubTokenKey = config.GitTokenKey
		}
		if githubTokenKey == "" {
			githubTokenKey = "token"
		}
		refs = append(refs, CredentialEnvRef{Name: "GITHUB_TOKEN", Secret: githubSecret, Key: githubTokenKey})
	}
	// The rebase model-gateway key rides the same shared enumeration (#732):
	// one SecretKeyRef the evidence intake re-scans, injected only when the
	// capability is enabled for a rebase run and a Secret is configured;
	// empty means an unauthenticated gateway. A credential only the rebase
	// path reads never rides a fix-pr, resolve-issue, or contribute pod.
	if config.RebaseModelAssist && mode == courierv1alpha1.ModeRebase && strings.TrimSpace(config.ModelGatewayKeySecret) != "" {
		keyName := config.ModelGatewayKeyName
		if keyName == "" {
			keyName = "key"
		}
		refs = append(refs, CredentialEnvRef{Name: "COURIER_MODEL_GATEWAY_API_KEY", Secret: config.ModelGatewayKeySecret, Key: keyName})
	}
	return refs
}

func podEnvironment(invocation Invocation, executorName string, config PodConfig) []corev1.EnvVar {
	remoteURL, baseRemoteURL := configuredRemotes(config, invocation)
	terminationFile := runtimePath + "/termination"
	// Validate and the launcher both check the trimmed URL, so the pod env
	// must carry the trimmed value too: a caller that bypasses the launcher
	// must not be able to pad the intake URL with whitespace.
	values := toKubernetesEnv(EnvironmentWithConfig(invocation, executorName, remoteURL, baseRemoteURL, config.BaseBranch, config.OpenCode.Binary, config.OpenCode.Format, terminationFile, config.OpenCode.Agent, strings.TrimSpace(config.EvidenceURL), config.EvidenceNonce, config.Git))
	values = append(values,
		corev1.EnvVar{
			Name:  "OPENCODE_CONFIG",
			Value: opencodeConfigMountPath + "/" + opencodeConfigFilename,
		},
		corev1.EnvVar{Name: "TMPDIR", Value: scratchPath},
		corev1.EnvVar{Name: "TMP", Value: scratchPath},
		corev1.EnvVar{Name: "TEMP", Value: scratchPath},
		corev1.EnvVar{Name: "COURIER_SCRATCH_DIR", Value: scratchPath},
		corev1.EnvVar{Name: "COURIER_TOOLCHAIN_DIR", Value: ToolchainReferencePath},
	)
	// Rebase model assist is opt-in operator configuration (#732): the
	// variables are emitted only on rebase-run pods when the capability is
	// enabled, because their absence is the meaningful fail-closed default —
	// the executor parses everything but an explicit affirmative as off.
	if config.RebaseModelAssist && invocation.Mode == courierv1alpha1.ModeRebase {
		values = append(values, corev1.EnvVar{Name: "COURIER_REBASE_MODEL_ASSIST", Value: "enabled"})
		if gatewayURL := strings.TrimSpace(config.ModelGatewayURL); gatewayURL != "" {
			values = append(values, corev1.EnvVar{Name: "COURIER_MODEL_GATEWAY_URL", Value: gatewayURL})
		}
	}
	// App-mode pods (#871 as enforced by #902) carry neither the installation
	// contract nor any projected token here: the model container holds no
	// git credential and no means to read one. Its remotes were already
	// rewritten to the git egress Service, and the guard pod — not this one —
	// reads the run's installation-token document and binds every write to
	// the approved work ref.
	// Per-key credential references come from the shared helper so the pod
	// builder and the evidence intake can never enumerate the same Secret keys
	// differently.
	for _, ref := range CredentialEnvRefs(config, invocation.Mode) {
		values = append(values, corev1.EnvVar{
			Name: ref.Name,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: ref.Secret}, Key: ref.Key,
			}},
		})
	}
	// Evidence env is appended independently of the credential logic so a
	// capture is wired even on a run with no git secrets.
	if config.EvidenceEnabled() {
		values = append(values,
			corev1.EnvVar{Name: EnvEvidenceToken, Value: config.EvidenceToken},
			corev1.EnvVar{Name: EnvPodUID, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.uid",
			}}},
		)
	}
	return values
}

// escapedRepositoryPath keeps repository components in the configured URL's
// path. Interpolating the raw owner/name lets a malformed-but-schema-valid
// value inject query/fragment syntax or path traversal into the clone URL.
func escapedRepositoryPath(repo string) string {
	parts := strings.SplitN(repo, "/", 2)
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func toKubernetesEnv(values []EnvVar) []corev1.EnvVar {
	env := make([]corev1.EnvVar, 0, len(values))
	for _, value := range values {
		env = append(env, corev1.EnvVar{Name: value.Name, Value: value.Value})
	}
	return env
}

// GitGuardPodName derives the per-run guard pod's name. Pods are validated
// as DNS-1123 subdomains, and the derived name is a single label, so the
// whole name is held to the 63-character DNS-label bound: an overflowing run
// name is truncated and digest-suffixed with a digest of the full run name,
// the way the coordinator pod's name is bounded, so distinct run names stay
// distinct and the launcher's create-or-verify address stays stable per run.
func GitGuardPodName(runName string) string {
	base := strings.Trim(strings.ToLower(runName), "-")
	const suffix = "-git-guard"
	maxBase := 63 - len(suffix)
	if len(base) > maxBase {
		digest := sha256.Sum256([]byte(runName))
		hash := hex.EncodeToString(digest[:])[:8]
		prefix := base[:maxBase-len(hash)-1]
		prefix = strings.ReplaceAll(prefix, ".", "-")
		base = strings.TrimRight(prefix, "-") + "-" + hash
	} else {
		base = strings.ReplaceAll(base, ".", "-")
	}
	return base + suffix
}

// GitGuardServiceName derives the per-run guard Service's name on the
// RFC-1035 DNS-label scheme topology's Services use (topology imports
// executor, so the scheme lives here and topology aliases it — the same
// single-producer arrangement as RunLabelValue, pinned by parity tests).
func GitGuardServiceName(runName string) string {
	const suffix = "-git-guard"
	base := strings.ToLower(runName)
	if isDNSLabel(base) && len(base)+len(suffix) <= 63 {
		return base + suffix
	}
	digest := sha256.Sum256([]byte(runName))
	hash := hex.EncodeToString(digest[:])[:8]
	maxBase := 63 - len(suffix) - len(hash) - 1
	base = strings.ReplaceAll(strings.ToLower(runName), ".", "-")
	if base == "" || base[0] < 'a' || base[0] > 'z' {
		maxBase--
	}
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	base = strings.Trim(base, "-.")
	if base == "" {
		base = "run"
	} else if base[0] < 'a' || base[0] > 'z' {
		base = "r" + base
	}
	return base + "-" + hash + suffix
}

// GitGuardServiceDNS returns the in-cluster DNS name of the run's guard
// Service — the only address the App-mode coordinator's git remotes are
// rewritten to (#902 as reworked by #919).
func GitGuardServiceDNS(namespace, runName string) string {
	return GitGuardServiceName(runName) + "." + namespace + ".svc.cluster.local"
}

// GitCredentialsServiceAccountName is the run-scoped identity the guard pod
// reads the run's credentials Secret under, and the coordinator pod's own
// identity when the isolated observer is not. One producer: the launcher
// provisions the Role and RoleBinding under the same name.
func GitCredentialsServiceAccountName(runName string) string {
	return "courier-gitcreds-" + RunLabelValue(runName)
}

// isDNSLabelBase reports whether s is a usable body of a DNS-1035 label:
// non-empty, lowercase alphanumeric and hyphen characters only, a lowercase
// letter first, and an alphanumeric last.
func isDNSLabel(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' || value[len(value)-1] == '-' {
		return false
	}
	for i := range value {
		c := value[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

func podName(runName string) string {
	base := strings.Trim(strings.ToLower(runName), "-")
	const suffix = "-coordinator"
	maxBase := 253 - len(suffix)
	if len(base) > maxBase {
		digest := sha256.Sum256([]byte(runName))
		hash := hex.EncodeToString(digest[:])[:8]
		base = strings.TrimRight(base[:maxBase-len(hash)-1], "-") + "-" + hash
	}
	return base + suffix
}

// RunLabelValue is the value the run label (LabelRun) is written with, so a
// reader selecting on it matches the pods the operator creates. All runs in
// one namespace share the label, so the value must stay a correct run
// selector: it is sanitized to a label-safe form and, when that overflows
// the 63-character label cap, truncated and hash-suffixed with a digest of
// the full run name so two runs sharing a long prefix do not collide. The
// scheme mirrors topology.RunLabelValue, which renders the same label on
// secure pods; executor cannot import topology (topology imports executor),
// so the two hold the construction in parallel, and controller readers
// accept both schemes plus LegacyRunLabelValue, which pods created before
// the digest scheme carry.
func RunLabelValue(name string) string {
	sanitized := sanitizeLabelValue(name)
	const max = 63
	if len(sanitized) <= max {
		return sanitized
	}
	digest := sha256.Sum256([]byte(name))
	hash := hex.EncodeToString(digest[:])[:8]
	cut := max - len(hash) - 1
	return strings.TrimRight(sanitized[:cut], "-_.") + "-" + hash
}

// LegacyRunLabelValue is the run-label scheme that predates the digest:
// sanitized and truncated to 63 characters, so two run names sharing a
// 63-character prefix collide. Nothing writes it any more; readers accept
// it only so executor pods created before the digest scheme stay attributed
// to their run until they are replaced.
func LegacyRunLabelValue(name string) string {
	return labelValue(name)
}

// sanitizeLabelValue lowercases a value and replaces any character outside
// the allowed label set with a hyphen, trimming the ends. It returns
// "unknown" for an empty result.
func sanitizeLabelValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			b.WriteRune(char)
		} else {
			b.WriteByte('-')
		}
	}
	value = strings.Trim(b.String(), "-_.")
	if value == "" {
		return "unknown"
	}
	return value
}

// labelValue is the bounded non-identity form: sanitized and truncated to
// the 63-character label cap without a digest. It bounds labels that group
// rather than select run identity (executor, repo, mode, lane), and it is
// the legacy run-label scheme readers still accept via LegacyRunLabelValue.
func labelValue(value string) string {
	sanitized := sanitizeLabelValue(value)
	if len(sanitized) > 63 {
		sanitized = strings.TrimRight(sanitized[:63], "-_.")
	}
	return sanitized
}

func nonEmpty(value string) string { return strings.TrimSpace(value) }

const gitCredentialsTokenVolumeName = "git-credentials-token"

// gitCredentialsTokenVolume projects the pod's ServiceAccount token and the
// API-server CA bundle for the App-mode readers of the run's credentials
// document — the git egress guard and, when App-enabled, the observer seed
// and observer (#871, #902). The volume is mounted only in those containers,
// never in the model container. The kube-root-ca.crt ConfigMap is namespaced
// and always present; the token projection carries no secret material into
// the pod spec itself.
func gitCredentialsTokenVolume() corev1.Volume {
	return corev1.Volume{
		Name: gitCredentialsTokenVolumeName,
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{
				{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: int64Ptr(3600)}},
				{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: observerCAName}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
			},
		}},
	}
}

func boolPtr(value bool) *bool { return &value }

func int64Ptr(value int64) *int64 { return &value }

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// EvidenceEnabled reports whether failure-evidence capture is enabled for this
// configuration. Capture is all-or-nothing: the intake URL and the token must
// both be present. A partially configured setup is rejected in Validate rather
// than silently degrading, so this gate is only reached once the three fields
// are consistent.
func (c PodConfig) EvidenceEnabled() bool {
	return strings.TrimSpace(c.EvidenceURL) != "" && strings.TrimSpace(c.EvidenceToken) != ""
}

// Validate checks the deployment-facing parts of a PodConfig before a caller
// starts creating Pods. Build performs the same checks while constructing the
// actual object.
func (c PodConfig) Validate() error {
	if strings.TrimSpace(c.Image) == "" {
		return errors.New("executor: image is required")
	}
	if strings.TrimSpace(c.WorkspacePath) == "" || !strings.HasPrefix(c.WorkspacePath, "/") {
		return fmt.Errorf("executor: workspace path must be absolute: %q", c.WorkspacePath)
	}
	if pathsOverlap(c.WorkspacePath, scratchPath) {
		return errors.New("executor: workspace path must not overlap the scratch path")
	}
	if pathsOverlap(c.WorkspacePath, ToolchainCachePath) {
		return errors.New("executor: workspace path must not overlap the toolchain cache path")
	}
	if pathsOverlap(c.WorkspacePath, ToolchainReferencePath) {
		return errors.New("executor: workspace path must not overlap the toolchain reference path")
	}
	if err := c.Git.Validate(); err != nil {
		return err
	}
	// Capture is all-or-nothing. A half-configured evidence setup (for example
	// a nonce without the URL and token) would hand the intake a token it
	// cannot route, so fail closed instead of wiring a partial env.
	set := 0
	if strings.TrimSpace(c.EvidenceURL) != "" {
		set++
	}
	if strings.TrimSpace(c.EvidenceToken) != "" {
		set++
	}
	if strings.TrimSpace(c.EvidenceNonce) != "" {
		set++
	}
	if set != 0 && set != 3 {
		return errors.New("executor: evidence capture is partially configured: EvidenceURL, EvidenceToken, and EvidenceNonce must all be set or all empty")
	}
	if strings.TrimSpace(c.EvidenceURL) != "" {
		return ValidateEvidenceURL(c.EvidenceURL)
	}
	// Model-assisted rebase conflict resolution fails closed (#732): enabled
	// without a gateway URL is a configuration error, never a silent
	// fallback to mechanical-only behavior that would publish under
	// different rules than the deployment asked for.
	if c.RebaseModelAssist && strings.TrimSpace(c.ModelGatewayURL) == "" {
		return errors.New("executor: rebase model assist is enabled but COURIER_MODEL_GATEWAY_URL is not set")
	}
	if err := c.InstallationGit.Validate(); err != nil {
		return err
	}
	// An App-mode deployment must be able to rewrite its remote template to
	// the guard's loopback origin: fail at configuration time, not per pod.
	if c.InstallationGit != nil {
		if _, err := guardRemoteTemplate(c.GitRemoteURL, "git-guard.invalid:"+strconv.Itoa(GitEgressPort)); err != nil {
			return err
		}
	}
	return nil
}

// pathsOverlap reports whether two absolute paths are equal or one is a
// path-segment ancestor of the other.
func pathsOverlap(a, b string) bool {
	a = path.Clean(a)
	b = path.Clean(b)
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
