package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path"
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
	invocation, err := NewInvocation(run, lane, config.WorkspacePath)
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
				Env:                      podEnvironment(invocation, executorImpl.Name(), config),
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
	// A 45-second grace only exists to fit the evidence capture deadline; a
	// run that does not capture keeps the kubelet's default.
	observerEnabled := executorImpl.Name() == "opencode"
	if observerEnabled {
		pod.Annotations[ObserverContractAnnotation] = ObserverContractVersion
		pod.Spec.Volumes = append(pod.Spec.Volumes, observerVolumes()...)
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, observerSeedContainer(config, invocation, pod.Name, string(run.UID)))
		pod.Spec.Containers = append(pod.Spec.Containers, observerContainer(config, invocation, pod.Name, string(run.UID)))
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
// enumerates, so the intake registers it by a different route.
func CredentialEnvRefs(config PodConfig) []CredentialEnvRef {
	var refs []CredentialEnvRef
	if strings.TrimSpace(config.GitCredentialSecret) != "" {
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
	if githubSecret == "" {
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
	return refs
}

func podEnvironment(invocation Invocation, executorName string, config PodConfig) []corev1.EnvVar {
	repoForRemote := invocation.HeadRepo
	if strings.TrimSpace(repoForRemote) == "" {
		repoForRemote = invocation.Repo
	}
	remoteURL := config.GitRemoteURL
	if strings.Contains(remoteURL, "%s") {
		remoteURL = strings.Replace(remoteURL, "%s", escapedRepositoryPath(repoForRemote), 1)
	}
	// The base remote always points at the repository that owns the base
	// branch: for a fork fix-pr run, syncing against the fork's base would
	// merge a possibly stale fork main.
	baseRemoteURL := config.GitRemoteURL
	if strings.Contains(baseRemoteURL, "%s") {
		baseRemoteURL = strings.Replace(baseRemoteURL, "%s", escapedRepositoryPath(invocation.Repo), 1)
	}
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
	// Per-key credential references come from the shared helper so the pod
	// builder and the evidence intake can never enumerate the same Secret keys
	// differently.
	for _, ref := range CredentialEnvRefs(config) {
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
