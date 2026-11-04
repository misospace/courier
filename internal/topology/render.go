package topology

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
)

// GatewayKeyName is the fixed key of the per-run gateway Secret the operator
// copies from the deployment's model-gateway key Secret.
const GatewayKeyName = "key"

// CoordinatorUID is the non-root identity every secure pod runs as, matching
// the executor's identity contract.
const CoordinatorUID int64 = 65532

// CachePin is the operator-pinned dependency cache address (an IP literal and
// port). Zero means no cache is configured and the worker has no egress.
type CachePin struct {
	IP   string
	Port int32
}

// GatewayPin is the optional model-gateway egress slot for control. The
// native model client (#124) supplies its configuration; until then control
// has no gateway egress.
type GatewayPin struct {
	CIDR string
	Port int32
}

// SkillBundlesConfigKey is the key of the per-run skill bundle config
// ConfigMap: the rendered config document the control pod resolves its
// pinned skills against. It is the file name inside the config mount, so the
// document lands at exactly executor.SkillBundlesConfigPath.
const SkillBundlesConfigKey = "bundles.yaml"

// SkillMount describes one materialized skill bundle mounted into a run pod:
// the ConfigMap carrying the bundle's content, the absolute path the
// deployment registered for it, and the projection's key-to-path items.
//
// Skill content is deployment-approved prompt and asset material the run
// deliberately exposes to its pods, never credential material, so it travels
// in a ConfigMap. A ConfigMap volume is neither a Secret nor a projected
// volume, so the worker's no-identity-material posture is untouched.
type SkillMount struct {
	// Name is the bundle's registered logical name.
	Name string
	// ConfigMapName is the per-run ConfigMap carrying the bundle's content.
	ConfigMapName string
	// Source is the absolute path the deployment registered for the
	// bundle. The projection is mounted exactly there, so the bundle's
	// files are readable where the registration says they are.
	Source string
	// Items maps the ConfigMap's data keys to the bundle's relative paths,
	// so the mounted projection carries the bundle's own file layout.
	Items []corev1.KeyToPath
}

// CredentialsSecretKeys is the fixed key contract of the per-run credential
// copy Secret the launcher provisions from the selected registration's
// references. The source Secret values are never referenced directly by run
// pods, and the copy is garbage-collected with the run.
const (
	CredKeyForgeAPIToken = "forge-api-token"
	CredKeyGitUsername   = "git-username"
	CredKeyGitToken      = "git-token"
)

// Labels returns the standard label set for one component of one run. The
// run label value is bounded and sanitized (RunLabelValue) so every rendered
// object passes Kubernetes label validation for every supported CoderRun
// name.
func Labels(runName, component string) map[string]string {
	return map[string]string{
		LabelRun:                          RunLabelValue(runName),
		"courier.misospace.dev/component": component,
		labelAppComponent:                 component,
		labelAppManagedBy:                 "courier",
	}
}

func runOwnerRef(run *courier.CoderRun) metav1.OwnerReference {
	controller := true
	block := true
	return metav1.OwnerReference{
		APIVersion:         courier.GroupVersion.String(),
		Kind:               "CoderRun",
		Name:               run.Name,
		UID:                run.UID,
		Controller:         &controller,
		BlockOwnerDeletion: &block,
	}
}

func objectMeta(run *courier.CoderRun, name, component string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       run.Namespace,
		Labels:          Labels(run.Name, component),
		OwnerReferences: []metav1.OwnerReference{runOwnerRef(run)},
	}
}

func podSecurityContext() *corev1.PodSecurityContext {
	uid := CoordinatorUID
	return &corev1.PodSecurityContext{
		RunAsNonRoot:   boolPtr(true),
		RunAsUser:      &uid,
		RunAsGroup:     &uid,
		FSGroup:        &uid,
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

func containerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: boolPtr(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// podResources returns the container sizing a builder must render, by
// delegating to courier.EffectivePodResources so the sizing policy lives in
// exactly one place: a caller that configures nothing gets the built-in
// default, and a caller that configures either half gets it with finite CPU
// and memory requests floored in, so no Courier-created pod is BestEffort
// and that invariant cannot depend on any call site. The rendered value is a
// deep copy: the argument's maps belong to the caller's LaneProfile read, and
// a rendered pod must not alias them.
func podResources(in corev1.ResourceRequirements) corev1.ResourceRequirements {
	return courier.EffectivePodResources(in)
}

func boolPtr(value bool) *bool { return &value }

func int64Ptr(v int64) *int64 { return &v }

func secretKeyRef(name, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
		Key:                  key,
	}}
}

func fieldRef(path string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}
}

// ServiceAccounts renders the three per-run service accounts. None of them
// automounts a token: control projects its own broker-audience token, the
// broker projects its own API token, and the worker receives no token at all.
func ServiceAccounts(run *courier.CoderRun) []*corev1.ServiceAccount {
	automount := false
	out := make([]*corev1.ServiceAccount, 0, 3)
	for _, spec := range []struct{ name, component string }{
		{ControlSAName(run.Name), ComponentCoordinator},
		{BrokerSAName(run.Name), ComponentBroker},
		{WorkerSAName(run.Name), ComponentWorker},
	} {
		sa := &corev1.ServiceAccount{
			ObjectMeta:                   objectMeta(run, spec.name, spec.component),
			AutomountServiceAccountToken: &automount,
		}
		out = append(out, sa)
	}
	return out
}

// BrokerRole renders the run-scoped Role for the broker's service account:
// named reads and status patches on exactly this CoderRun and namespace pod
// reads for current-incarnation checks. TokenReview creation is
// cluster-scoped and granted through BrokerTokenReviewBinding.
func BrokerRole(run *courier.CoderRun) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: objectMeta(run, BrokerRoleName(run.Name), ComponentBroker),
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{courier.GroupVersion.Group},
				Resources:     []string{"coderruns"},
				ResourceNames: []string{run.Name},
				Verbs:         []string{"get"},
			},
			{
				APIGroups:     []string{courier.GroupVersion.Group},
				Resources:     []string{"coderruns/status"},
				ResourceNames: []string{run.Name},
				Verbs:         []string{"get", "patch", "update"},
			},
			{
				// Named pod reads for current-incarnation checks (§3): the
				// three topology pods have deterministic names.
				APIGroups:     []string{""},
				Resources:     []string{"pods"},
				ResourceNames: []string{ControlPodName(run.Name), WorkerPodName(run.Name), BrokerPodName(run.Name)},
				Verbs:         []string{"get"},
			},
			{
				// List is required by the authenticator's
				// exactly-one-live-control scan; RBAC cannot name-scope a
				// list. The run namespace is dedicated, so this scan only
				// ever sees this run's pods.
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"list"},
			},
		},
	}
}

// BrokerRoleBinding binds the broker Role to the run's broker service
// account.
func BrokerRoleBinding(run *courier.CoderRun) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: objectMeta(run, BrokerRoleBindingName(run.Name), ComponentBroker),
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     BrokerRoleName(run.Name),
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      BrokerSAName(run.Name),
			Namespace: run.Namespace,
		}},
	}
}

// BrokerTokenReviewBinding binds the deployment-level ClusterRole that grants
// only TokenReview creation to this run's broker service account. The
// ClusterRole itself is provisioned by the chart. clusterRoleName is the
// chart-rendered ClusterRole for this deployment: side-by-side releases
// render distinct names so the fixed cluster-scoped name never collides
// (#632); an empty name falls back to the historical default.
func BrokerTokenReviewBinding(run *courier.CoderRun, clusterRoleName string) *rbacv1.ClusterRoleBinding {
	if clusterRoleName == "" {
		clusterRoleName = TokenReviewClusterRole
	}
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            run.Namespace + "." + BrokerTokenReviewBindingName(run.Name),
			Labels:          map[string]string{labelAppManagedBy: "courier"},
			OwnerReferences: []metav1.OwnerReference{runOwnerRef(run)},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     clusterRoleName,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      BrokerSAName(run.Name),
			Namespace: run.Namespace,
		}},
	}
}

// BrokerTLSSecret renders the per-run broker transport identity. Only the
// broker mounts the server key; control mounts the CA certificate.
func BrokerTLSSecret(run *courier.CoderRun, tls *BrokerTLS) (*corev1.Secret, error) {
	if tls == nil || len(tls.CACertPEM) == 0 || len(tls.ServerCertPEM) == 0 || len(tls.ServerKeyPEM) == 0 {
		return nil, errors.New("topology: broker TLS material is incomplete")
	}
	return &corev1.Secret{
		ObjectMeta: objectMeta(run, BrokerTLSSecretName(run.Name), ComponentBroker),
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"tls.crt": tls.ServerCertPEM,
			"tls.key": tls.ServerKeyPEM,
			"ca.crt":  tls.CACertPEM,
		},
	}, nil
}

// SigningSecret renders the per-control-incarnation Ed25519 private key.
// Only the control pod mounts it; the public half travels to the worker as
// plain pod-creation configuration, never as a Secret.
func SigningSecret(run *courier.CoderRun, privateKey ed25519.PrivateKey) (*corev1.Secret, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("topology: signing private key has the wrong size")
	}
	return &corev1.Secret{
		ObjectMeta: objectMeta(run, SigningSecretName(run.Name), ComponentCoordinator),
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"signing.key": privateKey,
		},
	}, nil
}

// PolicySecret renders the operator-resolved immutable run policy consumed by
// the broker. The document binds the persisted PublicationPolicy and the
// selected registration projection; it contains references only.
func PolicySecret(run *courier.CoderRun, document BrokerPolicyDocument) (*corev1.Secret, error) {
	data, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("topology: marshal broker policy document: %w", err)
	}
	return &corev1.Secret{
		ObjectMeta: objectMeta(run, PolicySecretName(run.Name), ComponentBroker),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"policy.json": data},
	}, nil
}

// BrokerPolicyDocument is the on-wire shape of the broker's policy Secret:
// the persisted publication policy plus the single projected registration.
type BrokerPolicyDocument struct {
	PublicationPolicy *courier.PublicationPolicy `json:"publicationPolicy"`
	Provider          forge.Projection           `json:"provider"`
}

// GatewaySecret renders the per-run copy of the deployment's model-gateway
// key. The launcher copies the value from the deployment's key Secret; run GC
// deletes the copy and never a shared source. Only trusted control mounts it.
func GatewaySecret(run *courier.CoderRun, key []byte) (*corev1.Secret, error) {
	if len(key) == 0 {
		return nil, errors.New("topology: gateway key copy requires a key value")
	}
	return &corev1.Secret{
		ObjectMeta: objectMeta(run, GatewaySecretName(run.Name), ComponentCoordinator),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{GatewayKeyName: key},
	}, nil
}

// CredentialsSecret renders the per-run copy of the selected registration's
// credential values under the fixed key contract. The launcher resolves the
// referenced values from the deployment's Secrets; run GC deletes the copy
// and never a shared source.
func CredentialsSecret(run *courier.CoderRun, forgeAPIToken, gitUsername, gitToken string) (*corev1.Secret, error) {
	if forgeAPIToken == "" || gitToken == "" {
		return nil, errors.New("topology: credential copy requires the forge-api and git token values")
	}
	if gitUsername == "" {
		gitUsername = DefaultGitUsername
	}
	return &corev1.Secret{
		ObjectMeta: objectMeta(run, CredentialsSecretName(run.Name), ComponentBroker),
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			CredKeyForgeAPIToken: []byte(forgeAPIToken),
			CredKeyGitUsername:   []byte(gitUsername),
			CredKeyGitToken:      []byte(gitToken),
		},
	}, nil
}

// BrokerService renders the run's ClusterIP Service. It is the only Service
// fronting the broker, and it is never shared across runs.
func BrokerService(run *courier.CoderRun) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: objectMeta(run, BrokerServiceName(run.Name), ComponentBroker),
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				LabelRun:                          RunLabelValue(run.Name),
				"courier.misospace.dev/component": ComponentBroker,
			},
			Ports: []corev1.ServicePort{
				{Name: brokerPortName, Port: 8443, TargetPort: intstr.FromInt32(8443), Protocol: corev1.ProtocolTCP},
				{Name: brokerStatusPortNm, Port: 8444, TargetPort: intstr.FromInt32(8444), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

// WorkerService renders a headless Service purely so trusted control can
// address the worker by a stable DNS name without reading the API. It grants
// no ingress beyond the worker's NetworkPolicy.
func WorkerService(run *courier.CoderRun) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: objectMeta(run, WorkerServiceName(run.Name), ComponentWorker),
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: "None",
			Selector: map[string]string{
				LabelRun:                          RunLabelValue(run.Name),
				"courier.misospace.dev/component": ComponentWorker,
			},
			Ports: []corev1.ServicePort{
				{Name: workerPortName, Port: 8080, TargetPort: intstr.FromInt32(WorkerPort), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

// SkillContentConfigMap renders the per-run ConfigMap carrying one pinned
// skill bundle's materialized content. The data keys are encoded (f0, f1,
// ...) in sorted relative-path order because a ConfigMap's key names must
// match [-._a-zA-Z0-9]+ while a bundle's file paths may not; the key-to-path
// mapping rides the pod's volume projection, which places every file at its
// relative path inside the mount.
//
// The content rides binaryData: a bundle file is arbitrary bytes, and only
// the binary form carries them exactly.
func SkillContentConfigMap(run *courier.CoderRun, skillName string, data map[string][]byte) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(run, SkillContentConfigMapName(run.Name, skillName), ComponentCoordinator),
		BinaryData: data,
	}
}

// SkillBundlesConfigMap renders the per-run ConfigMap carrying the rendered
// skill bundle config the control pod resolves its pinned skills against.
func SkillBundlesConfigMap(run *courier.CoderRun, document []byte) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(run, SkillBundlesConfigMapName(run.Name), ComponentCoordinator),
		BinaryData: map[string][]byte{SkillBundlesConfigKey: document},
	}
}

// renderSkillVolumes renders the pod volumes and volume mounts for one run's
// materialized skill bundles: one ConfigMap projection per bundle, mounted
// read-only at the bundle's registered source path. The mounts are read-only
// because bundle content is operator-authored data the pods read and never
// write — the worker must not be able to alter what a pinned skill says.
//
// A ConfigMap volume is not a Secret volume and not a projected volume, so
// a pinned bundle adds no credential, no token, and no identity material to
// the pod that mounts it.
func renderSkillVolumes(mounts []SkillMount) ([]corev1.Volume, []corev1.VolumeMount) {
	volumes := make([]corev1.Volume, 0, len(mounts))
	volumeMounts := make([]corev1.VolumeMount, 0, len(mounts))
	for _, mount := range mounts {
		name := skillVolumeName(mount.Name)
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: mount.ConfigMapName},
				Items:                mount.Items,
			}},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      name,
			MountPath: mount.Source,
			ReadOnly:  true,
		})
	}
	return volumes, volumeMounts
}

// renderSkillConfigVolume renders the control pod's bundle config volume:
// the per-run ConfigMap carrying the rendered config, mounted read-only at
// the directory that places the file exactly at
// executor.SkillBundlesConfigPath. The worker needs no config — it reads
// the bundle content, never the pins.
func renderSkillConfigVolume(configMapName string) (corev1.Volume, corev1.VolumeMount) {
	const volumeName = "skill-bundles-config"
	return corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
			Items:                []corev1.KeyToPath{{Key: SkillBundlesConfigKey, Path: SkillBundlesConfigKey}},
		}},
	}, corev1.VolumeMount{
		Name:      volumeName,
		MountPath: filepath.Dir(executor.SkillBundlesConfigPath),
		ReadOnly:  true,
	}
}

// dnsSafeTag derives a short DNS-safe tag for a name: a lowercase
// alphanumeric-and-dash prefix plus an 8-hex digest of the full name, within
// a 16-character budget. The normalized prefix is readable only — distinct
// names share long prefixes and can normalize to the same string — so the
// digest of the full name always follows it and keeps the tag one name's own.
func dnsSafeTag(name string) string {
	const budget = 16
	prefix := strings.Trim(strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return -1
		}
	}, name), "-")
	sum := sha256.Sum256([]byte(name))
	digest := hex.EncodeToString(sum[:])[:8]
	if len(prefix) > budget-len(digest) {
		prefix = prefix[:budget-len(digest)]
	}
	return prefix + digest
}

// skillVolumeName derives the pod volume name of one materialized skill
// bundle. A volume name is a DNS label, and the digest in dnsSafeTag means
// two distinct skill names can never render the same volume, whatever
// prefix they share.
func skillVolumeName(skillName string) string {
	return "skill-" + dnsSafeTag(skillName)
}

// WorkerPod renders the untrusted sandbox worker: no secret, certificate,
// token, or signing key; a dedicated unbound service account with token
// automount off; restartPolicy Never so replay state cannot silently reset.
//
// controlIncarnationUID is the operator-minted identifier of the current
// control incarnation. Signed envelopes must carry exactly this value; a
// control or worker replacement always mints a new one, so stale envelopes
// are rejected by construction.
//
// resources is the lane-configured sizing; the zero value renders the
// built-in default rather than a BestEffort pod.
//
// skills are the run's materialized skill bundles: each is mounted read-only
// at the bundle's registered source path, so a skill's supporting assets are
// readable where the worker's shell runs. They add no Secret reference, no
// token, no projected volume, no environment value, no service account, and
// no writable path — a bundle is read-only content the worker may read and
// never alter. The content itself is deployment-approved prompt and asset
// material the run deliberately exposes, not credential material, so it
// travels in a read-only ConfigMap volume: the worker's volumes stay free of
// anything that could identify it.
//
// image is the caller-resolved worker runtime: the deployment harness image
// by default, or the lane's RuntimeImage. Whatever it names must carry
// /usr/local/bin/courier-worker; the mounts below cannot shadow baked tool
// installations because tools install outside the toolchain cache paths
// (/usr/local/go, /usr/local/bin) — the contract the Dockerfile pins.
//
// A bundle's registered source path may not collide with a path the
// operator's own pods own, and no two bundles may share one: the render
// rejects such a registration before the pod is created, so the worker never
// comes up with a shadowed or duplicated mount.
func WorkerPod(run *courier.CoderRun, image string, controlIncarnationUID string, publicKey ed25519.PublicKey, cache *CachePin, resources corev1.ResourceRequirements, skills []SkillMount) (*corev1.Pod, error) {
	if controlIncarnationUID == "" {
		return nil, errors.New("topology: worker pod requires the control incarnation identifier")
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, errors.New("topology: worker public key has the wrong size")
	}
	if err := ValidateSkillMounts(skills); err != nil {
		return nil, err
	}
	env := []corev1.EnvVar{
		{Name: EnvRunUID, Value: string(run.UID)},
		{Name: EnvControlPodUID, Value: controlIncarnationUID},
		{Name: EnvWorkerPodUID, ValueFrom: fieldRef("metadata.uid")},
		{Name: EnvWorkerPodName, ValueFrom: fieldRef("metadata.name")},
		{Name: EnvWorkerPublicKey, Value: base64.StdEncoding.EncodeToString(publicKey)},
		// The same toolchain reference the legacy coordinator env carries.
		{Name: "COURIER_TOOLCHAIN_DIR", Value: executor.ToolchainReferencePath},
	}
	if cache != nil && cache.IP != "" {
		env = append(env,
			corev1.EnvVar{Name: EnvGoProxy, Value: "http://" + cache.IP + ":" + strconv.FormatInt(int64(cache.Port), 10)},
			corev1.EnvVar{Name: EnvGoSumDB, Value: "off"},
		)
	}
	volumes := []corev1.Volume{{
		Name:         "workspace",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}, {
		// Per-pod scratch caches (GOMODCACHE, GOCACHE): recreated with
		// the pod, writable through the pod's fsGroup identity, and
		// read-only at the reference twin. Durable shared writable
		// storage is deliberately out of scope (#415).
		Name:         "toolchain-cache",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	mounts := []corev1.VolumeMount{{
		Name:      "workspace",
		MountPath: workerWorkspacePath,
	}, {
		Name:      "toolchain-cache",
		MountPath: executor.ToolchainCachePath,
	}, {
		Name:      "toolchain-cache",
		MountPath: executor.ToolchainReferencePath,
		ReadOnly:  true,
	}}
	skillVolumes, skillMounts := renderSkillVolumes(skills)
	volumes = append(volumes, skillVolumes...)
	mounts = append(mounts, skillMounts...)
	return &corev1.Pod{
		ObjectMeta: objectMeta(run, WorkerPodName(run.Name), ComponentWorker),
		Spec: corev1.PodSpec{
			ServiceAccountName:           WorkerSAName(run.Name),
			AutomountServiceAccountToken: boolPtr(false),
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext:              podSecurityContext(),
			Volumes:                      volumes,
			Containers: []corev1.Container{{
				Name:    WorkerContainerName,
				Image:   image,
				Command: []string{"/usr/local/bin/courier-worker"},
				Args:    []string{"--addr=" + strconv.Itoa(WorkerPort), "--workspace=" + workerWorkspacePath},
				Env:     env,
				Ports: []corev1.ContainerPort{{
					Name:          workerPortName,
					ContainerPort: WorkerPort,
					Protocol:      corev1.ProtocolTCP,
				}},
				VolumeMounts:    mounts,
				Resources:       podResources(resources),
				SecurityContext: containerSecurityContext(),
			}},
		},
	}, nil
}

// BrokerPod renders the trusted per-run broker pod: TLS identity, the frozen
// one-registration projection, its own projected API token, and the resolved
// credential references — nothing else.
//
// resources is the lane-configured sizing; the zero value renders the
// built-in default rather than a BestEffort pod.
func BrokerPod(run *courier.CoderRun, image string, projection forge.Projection, controlSAUID string, resources corev1.ResourceRequirements) (*corev1.Pod, error) {
	if controlSAUID == "" {
		return nil, errors.New("topology: broker pod requires the control service account UID")
	}
	if projection.Name == "" || projection.Endpoint == "" || projection.GitTemplate == "" {
		return nil, errors.New("topology: broker pod requires a complete provider projection")
	}
	env := []corev1.EnvVar{
		{Name: EnvPodNamespace, Value: run.Namespace},
		{Name: EnvBrokerPodName, ValueFrom: fieldRef("metadata.name")},
		{Name: EnvBrokerPodUID, ValueFrom: fieldRef("metadata.uid")},
		{Name: EnvControlSA, Value: ControlSAName(run.Name)},
		{Name: EnvControlSAUID, Value: controlSAUID},
		{Name: EnvProviderName, Value: projection.Name},
		{Name: EnvProviderType, Value: projection.Type},
		{Name: EnvProviderEndpoint, Value: projection.Endpoint},
		{Name: EnvProviderGitEndp, Value: projection.GitTemplate},
		{Name: EnvForgeAPIToken, ValueFrom: secretKeyRef(CredentialsSecretName(run.Name), CredKeyForgeAPIToken)},
		{Name: EnvGitUsername, ValueFrom: secretKeyRef(CredentialsSecretName(run.Name), CredKeyGitUsername)},
		{Name: EnvGitToken, ValueFrom: secretKeyRef(CredentialsSecretName(run.Name), CredKeyGitToken)},
		{Name: EnvAPITokenFile, Value: brokerTokenPath},
	}
	return &corev1.Pod{
		ObjectMeta: objectMeta(run, BrokerPodName(run.Name), ComponentBroker),
		Spec: corev1.PodSpec{
			ServiceAccountName:           BrokerSAName(run.Name),
			AutomountServiceAccountToken: boolPtr(false),
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext:              podSecurityContext(),
			Volumes: []corev1.Volume{
				{
					Name: "policy",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
						SecretName: PolicySecretName(run.Name),
						Items:      []corev1.KeyToPath{{Key: "policy.json", Path: "policy.json"}},
					}},
				},
				{
					Name: "tls",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
						SecretName: BrokerTLSSecretName(run.Name),
						Items: []corev1.KeyToPath{
							{Key: "tls.crt", Path: "tls.crt"},
							{Key: "tls.key", Path: "tls.key"},
						},
					}},
				},
				{
					// The broker's own pod-bound API token with the default
					// (API) audience, projected to a dedicated path. The
					// broker builds its API client from this file explicitly.
					Name: "api-token",
					VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
						Sources: []corev1.VolumeProjection{{
							ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
								Path: "token",
							},
						}},
					}},
				},
				{
					Name: "kube-root-ca",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
						Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
					}},
				},
			},
			Containers: []corev1.Container{{
				Name:    BrokerContainerName,
				Image:   image,
				Command: []string{"/usr/local/bin/courier-broker"},
				Args: []string{
					"--addr=" + brokerAPIAddr,
					"--status-addr=" + brokerStatusAddr,
					"--policy=" + brokerPolicyPath,
					"--tls-cert=" + brokerTLSCertPath,
					"--tls-key=" + brokerTLSKeyPath,
					"--scratch-dir=" + brokerScratchPath,
				},
				Env: env,
				Ports: []corev1.ContainerPort{
					{Name: brokerPortName, ContainerPort: 8443, Protocol: corev1.ProtocolTCP},
					{Name: brokerStatusPortNm, ContainerPort: 8444, Protocol: corev1.ProtocolTCP},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "policy", MountPath: "/var/run/courier/broker", ReadOnly: true},
					{Name: "tls", MountPath: "/var/run/courier/tls", ReadOnly: true},
					{Name: "api-token", MountPath: "/var/run/secrets/tokens/api", ReadOnly: true},
					{Name: "kube-root-ca", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
				},
				Resources:       podResources(resources),
				SecurityContext: containerSecurityContext(),
			}},
		},
	}, nil
}

// ControlInputs carries everything the control pod needs: the run-context
// invocation the operator assembled for the run, the resolved identity and
// addresses of this incarnation round, and the optional model-gateway
// configuration. The gateway key arrives through the per-run gateway Secret;
// GatewayKeyMounted reports whether that Secret was provisioned. Resources is
// the lane-configured sizing; the zero value renders the built-in default
// rather than a BestEffort pod.
//
// Skills are the run's materialized skill bundles, and SkillBundlesConfigMap
// is the per-run ConfigMap carrying the bundle config the pod resolves its
// pins against. Both are empty for a skillless run, which then renders no
// skill volume, no skill mount, and no config mount at all.
type ControlInputs struct {
	Image                 string
	Run                   executor.Invocation
	GatewayURL            string
	GatewayKeyMounted     bool
	ControlSAUID          string
	WorkerPodUID          string
	ControlIncarnationUID string
	WorkerURL             string
	BrokerURL             string
	BrokerStatusURL       string
	Resources             corev1.ResourceRequirements
	Skills                []SkillMount
	SkillBundlesConfigMap string
}

// ControlPod renders the trusted control pod. Its container is named
// "coordinator" so the operator's existing observation and termination
// contract applies unchanged, and it carries the component label the broker's
// authenticator expects. It mounts the signing private key, the broker CA,
// and its projected 600-second pod-bound broker-audience token — and no
// forge or git credential of any kind. It is the only pod that holds the
// model-provider key, and that key is mounted as a file, not an env value.
func ControlPod(run *courier.CoderRun, in ControlInputs) (*corev1.Pod, error) {
	if in.ControlSAUID == "" || in.WorkerPodUID == "" || in.ControlIncarnationUID == "" || in.WorkerURL == "" || in.BrokerURL == "" {
		return nil, errors.New("topology: control pod requires resolved identity and addresses")
	}
	if in.GatewayURL == "" && in.GatewayKeyMounted {
		return nil, errors.New("topology: control pod renders a gateway key Secret without a gateway URL")
	}
	if err := ValidateSkillMounts(in.Skills); err != nil {
		return nil, err
	}
	volumes := []corev1.Volume{
		{
			Name:         "workspace",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name:         "runtime",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name: "signing-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: SigningSecretName(run.Name),
				Items:      []corev1.KeyToPath{{Key: "signing.key", Path: "signing.key"}},
			}},
		},
		{
			Name: "broker-ca",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: BrokerTLSSecretName(run.Name),
				Items:      []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
			}},
		},
		{
			Name: "broker-token",
			VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
						Audience:          BrokerAudience,
						ExpirationSeconds: int64Ptr(ControlTokenSeconds),
						Path:              "broker-token",
					},
				}},
			}},
		},
	}
	mounts := []corev1.VolumeMount{
		{Name: "workspace", MountPath: controlWorkspacePath},
		{Name: "runtime", MountPath: controlRuntimePath},
		{Name: "signing-key", MountPath: "/var/run/courier/signing", ReadOnly: true},
		{Name: "broker-ca", MountPath: "/var/run/courier/broker", ReadOnly: true},
		{Name: "broker-token", MountPath: "/var/run/secrets/tokens", ReadOnly: true},
	}
	env := []corev1.EnvVar{
		{Name: EnvPodNamespace, Value: run.Namespace},
		{Name: EnvRunUID, Value: string(run.UID)},
		{Name: EnvControlPodUID, Value: in.ControlIncarnationUID},
		{Name: EnvControlKubeUID, ValueFrom: fieldRef("metadata.uid")},
		{Name: EnvWorkerPodUID, Value: in.WorkerPodUID},
		{Name: EnvWorkerURL, Value: in.WorkerURL},
		{Name: EnvBrokerURL, Value: in.BrokerURL},
		{Name: EnvBrokerStatusURL, Value: in.BrokerStatusURL},
		{Name: EnvBrokerCAFile, Value: controlBrokerCAPath},
		{Name: EnvSigningKeyFile, Value: controlSigningKeyPath},
		{Name: EnvBrokerTokenFile, Value: controlTokenPath},
		{Name: "COURIER_TERMINATION_FILE", Value: controlRuntimePath + "/termination"},
	}
	if in.GatewayURL != "" && in.GatewayKeyMounted {
		volumes = append(volumes, corev1.Volume{
			Name: "gateway-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: GatewaySecretName(run.Name),
				Items:      []corev1.KeyToPath{{Key: GatewayKeyName, Path: "key"}},
			}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "gateway-key", MountPath: "/var/run/courier/gateway", ReadOnly: true})
		env = append(env,
			corev1.EnvVar{Name: EnvGatewayURL, Value: in.GatewayURL},
			corev1.EnvVar{Name: EnvGatewayKeyFile, Value: controlGatewayKeyPath},
		)
	} else if in.GatewayURL != "" {
		// An unauthenticated gateway is a deployment choice: the URL is
		// still rendered, no key material exists.
		env = append(env, corev1.EnvVar{Name: EnvGatewayURL, Value: in.GatewayURL})
	}
	for _, runEnv := range executor.RunContextEnvironment(in.Run) {
		env = append(env, corev1.EnvVar{Name: runEnv.Name, Value: runEnv.Value})
	}
	// The run's materialized skill bundles and the config the pod resolves
	// its pins against are mounted read-only from ConfigMaps: the bundle
	// content is operator-authored data the pod reads, and the config is
	// the same document the operator rendered from the deployment
	// registry. A bundle's registered source path may not collide with a
	// path the operator's own pods own, and no two bundles may share one:
	// the render rejects such a registration before the pod is created.
	skillVolumes, skillMounts := renderSkillVolumes(in.Skills)
	volumes = append(volumes, skillVolumes...)
	mounts = append(mounts, skillMounts...)
	if in.SkillBundlesConfigMap != "" {
		configVolume, configMount := renderSkillConfigVolume(in.SkillBundlesConfigMap)
		volumes = append(volumes, configVolume)
		mounts = append(mounts, configMount)
	}
	return &corev1.Pod{
		ObjectMeta: objectMeta(run, ControlPodName(run.Name), ComponentCoordinator),
		Spec: corev1.PodSpec{
			ServiceAccountName:           ControlSAName(run.Name),
			AutomountServiceAccountToken: boolPtr(false),
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext:              podSecurityContext(),
			Volumes:                      volumes,
			Containers: []corev1.Container{{
				Name:                     ControlContainerName,
				Image:                    in.Image,
				Command:                  []string{"/usr/local/bin/courier-control"},
				WorkingDir:               controlWorkspacePath,
				TerminationMessagePath:   controlRuntimePath + "/termination",
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				Env:                      env,
				VolumeMounts:             mounts,
				Resources:                podResources(in.Resources),
				SecurityContext:          containerSecurityContext(),
			}},
		},
	}, nil
}

// LabelProbe marks disposable preflight probe pods. Probe pods deliberately
// carry the component label of the workload whose NetworkPolicy they exercise
// — the worker's for ProbePod, the broker's for BrokerProbePod — so the
// policy under test is the workload's own, not a special exception; this
// extra label lets the operator's workload lookups distinguish them.
const LabelProbe = "courier.misospace.dev/probe"

// ProbePod renders one disposable identity-less network probe pod pinned to a
// node. It carries the worker's labels so the worker's own NetworkPolicy —
// not a special exception — is what the probe exercises.
//
// Its sizing is the fixed probe value, not the lane's: preflight creates one
// probe per eligible node simultaneously, and a seconds-lived network check
// reserving lane-scale capacity can stay Pending on a request-tight cluster
// and wedge preflight (#402).
func ProbePod(run *courier.CoderRun, node, image, script string) *corev1.Pod {
	return probePod(run, node, image, script, ComponentWorker, WorkerSAName(run.Name), "-probe-")
}

// BrokerProbePod renders the broker's twin of ProbePod: the same disposable,
// identity-less network probe pod pinned to a node, but carrying the broker's
// component label and running as the broker's service account, so it
// exercises the broker's own NetworkPolicy egress — in particular the
// broker-to-API-server allow path preflight must prove.
//
// Its sizing is the fixed probe value for the same reason ProbePod's is.
func BrokerProbePod(run *courier.CoderRun, node, image, script string) *corev1.Pod {
	return probePod(run, node, image, script, ComponentBroker, BrokerSAName(run.Name), "-broker-probe-")
}

func probePod(run *courier.CoderRun, node, image, script, component, serviceAccount, suffix string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: objectMeta(run, podResourceName(run.Name, suffix+nodeNameTag(node)), component),
		Spec: corev1.PodSpec{
			NodeName:                     node,
			ServiceAccountName:           serviceAccount,
			AutomountServiceAccountToken: boolPtr(false),
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext:              podSecurityContext(),
			Containers: []corev1.Container{{
				Name:                     "probe",
				Image:                    image,
				Command:                  []string{"/bin/sh", "-c", script},
				TerminationMessagePath:   "/dev/termination-log",
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				Resources:                courier.ProbePodResources(),
				SecurityContext:          containerSecurityContext(),
			}},
		},
	}
	pod.Labels[LabelProbe] = "true"
	return pod
}

// nodeNameTag produces a short DNS-safe tag for a node name: dnsSafeTag's
// prefix-plus-digest keeps distinct nodes distinct however long their shared
// normalized prefix.
func nodeNameTag(node string) string {
	return dnsSafeTag(node)
}
