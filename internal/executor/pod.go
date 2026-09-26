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

	LabelRun       = "courier.misospace.dev/coderrun"
	LabelComponent = "courier.misospace.dev/component"
	LabelExecutor  = "courier.misospace.dev/executor"
	LabelRepo      = "courier.misospace.dev/repo"
	LabelMode      = "courier.misospace.dev/mode"
	LabelLane      = "courier.misospace.dev/lane"
	LabelDebug     = "courier.misospace.dev/debug"
	runtimePath    = "/courier-runtime"
	scratchPath    = "/var/tmp/courier-scratch"
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
	GitHubMCPURL           string
	Context7MCPURL         string
	MetricsMCPURL          string
	BootstrapBinary        string
	OpenCode               OpenCode
	Resources              corev1.ResourceRequirements
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
		LabelRun:                       labelValue(run.Name),
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
			}},
			Containers: []corev1.Container{{
				Name:            "coordinator",
				Image:           config.Image,
				ImagePullPolicy: config.ImagePullPolicy,
				Command:         []string{command.Binary},
				Args:            append([]string(nil), command.Args...),
				WorkingDir:      config.WorkspacePath,
				Env:             podEnvironment(invocation, executorImpl.Name(), config),
				Resources:       config.Resources,
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
				}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: boolPtr(false),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}
	if strings.TrimSpace(config.EnvironmentSecret) != "" {
		pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: config.EnvironmentSecret}},
		}}
	}
	return pod, nil
}

func podEnvironment(invocation Invocation, executorName string, config PodConfig) []corev1.EnvVar {
	remoteURL := config.GitRemoteURL
	if strings.Contains(remoteURL, "%s") {
		remoteURL = strings.Replace(remoteURL, "%s", escapedRepositoryPath(invocation.Repo), 1)
	}
	terminationFile := runtimePath + "/termination"
	values := toKubernetesEnv(EnvironmentWithConfig(invocation, executorName, remoteURL, config.BaseBranch, config.OpenCode.Binary, config.OpenCode.Format, terminationFile, config.OpenCode.Agent))
	values = append(values,
		corev1.EnvVar{
			Name:  "OPENCODE_CONFIG",
			Value: opencodeConfigMountPath + "/" + opencodeConfigFilename,
		},
		corev1.EnvVar{Name: "TMPDIR", Value: scratchPath},
		corev1.EnvVar{Name: "TMP", Value: scratchPath},
		corev1.EnvVar{Name: "TEMP", Value: scratchPath},
		corev1.EnvVar{Name: "COURIER_SCRATCH_DIR", Value: scratchPath},
	)
	githubSecret := config.GitHubCredentialSecret
	if githubSecret == "" {
		githubSecret = config.GitCredentialSecret
	}
	if strings.TrimSpace(config.GitCredentialSecret) == "" && strings.TrimSpace(githubSecret) == "" {
		return values
	}
	env := values
	if strings.TrimSpace(config.GitCredentialSecret) != "" {
		usernameKey := config.GitUsernameKey
		if usernameKey == "" {
			usernameKey = "username"
		}
		tokenKey := config.GitTokenKey
		if tokenKey == "" {
			tokenKey = "token"
		}
		env = append(env,
			corev1.EnvVar{Name: "COURIER_GIT_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: config.GitCredentialSecret}, Key: usernameKey,
			}}},
			corev1.EnvVar{Name: "COURIER_GIT_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: config.GitCredentialSecret}, Key: tokenKey,
			}}},
		)
	}
	if strings.TrimSpace(githubSecret) != "" {
		githubTokenKey := config.GitHubTokenKey
		if githubTokenKey == "" {
			githubTokenKey = config.GitTokenKey
			if githubTokenKey == "" {
				githubTokenKey = "token"
			}
		}
		env = append(env, corev1.EnvVar{Name: "GITHUB_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: githubSecret}, Key: githubTokenKey,
		}}})
	}
	return env
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

func labelValue(value string) string {
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
	if len(value) > 63 {
		value = strings.TrimRight(value[:63], "-_.")
	}
	return value
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
