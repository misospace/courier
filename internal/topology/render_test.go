package topology

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/protocol"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courier "github.com/misospace/courier/api/v1alpha1"
)

func testRun(name string) *courier.CoderRun {
	return &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("run-uid-" + name)},
		Spec: courier.CoderRunSpec{
			Mode:   courier.ModeResolveIssue,
			Source: "manual",
			Repo:   "misospace/courier",
			Ref:    1,
			Lane:   "default",
		},
	}
}

func credentialEnvs(pod *corev1.Pod) map[string]corev1.EnvVarSource {
	out := map[string]corev1.EnvVarSource{}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.ValueFrom != nil {
			out[env.Name] = *env.ValueFrom
		}
	}
	return out
}

func secretVolumes(pod *corev1.Pod) map[string]string {
	out := map[string]string{}
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			out[v.Name] = v.Secret.SecretName
		}
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.ServiceAccountToken != nil {
					out[v.Name] = "service-account-token"
				}
				if src.Secret != nil {
					out[v.Name] = src.Secret.Name
				}
			}
		}
	}
	return out
}

func TestWorkerPodHasNoIdentityMaterial(t *testing.T) {
	run := testRun("worker-clean")
	pub, _, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("worker pod must not automount a service account token")
	}
	if pod.Spec.ServiceAccountName != WorkerSAName(run.Name) {
		t.Fatalf("worker service account = %q", pod.Spec.ServiceAccountName)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("worker restart policy = %q", pod.Spec.RestartPolicy)
	}
	if vols := secretVolumes(pod); len(vols) != 0 {
		t.Fatalf("worker pod must mount no secret, token, or projected volume: %v", vols)
	}
	for _, v := range pod.Spec.Volumes {
		if v.EmptyDir == nil {
			t.Fatalf("worker volume %q must be an emptyDir", v.Name)
		}
	}
	envs := credentialEnvs(pod)
	for _, forbidden := range []string{EnvForgeAPIToken, EnvGitToken, EnvGitUsername, EnvBrokerTokenFile, EnvSigningKeyFile} {
		if _, ok := envs[forbidden]; ok {
			t.Fatalf("worker pod carries forbidden env %q", forbidden)
		}
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			t.Fatalf("worker env %q references a Secret", env.Name)
		}
	}
	if pod.Spec.Containers[0].Command[0] != "/usr/local/bin/courier-worker" {
		t.Fatalf("worker argv[0] = %v", pod.Spec.Containers[0].Command)
	}
	if pod.Labels["courier.misospace.dev/component"] != ComponentWorker {
		t.Fatalf("worker component label = %q", pod.Labels["courier.misospace.dev/component"])
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].UID != run.UID || pod.OwnerReferences[0].Controller == nil || !*pod.OwnerReferences[0].Controller {
		t.Fatal("worker pod must be controller-owned by the CoderRun")
	}
}

func TestWorkerPodCachePin(t *testing.T) {
	run := testRun("worker-cache")
	pub, _, _ := protocol.GenerateKey()
	pod, err := WorkerPod(run, "harness:test", "incarnation-1", pub, &CachePin{IP: "10.0.0.42", Port: 5120})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, env := range pod.Spec.Containers[0].Env {
		found[env.Name] = env.Value
	}
	if found[EnvGoProxy] != "http://10.0.0.42:5120" {
		t.Fatalf("GOPROXY = %q", found[EnvGoProxy])
	}
	if found[EnvGoSumDB] != "off" {
		t.Fatalf("GOSUMDB = %q", found[EnvGoSumDB])
	}
}

func TestControlPodIdentityContract(t *testing.T) {
	run := testRun("control-pod")
	pod, err := ControlPod(run, ControlInputs{
		Image:                 "harness:test",
		ControlSAUID:          "sa-uid",
		WorkerPodUID:          "worker-uid",
		ControlIncarnationUID: "incarnation-1",
		WorkerURL:             "http://r-worker.ns.svc.cluster.local:8080",
		BrokerURL:             "https://r-broker.ns.svc.cluster.local:8443",
	})
	if err != nil {
		t.Fatal(err)
	}
	// The operator's observation contract and the broker's authenticator both
	// key on the "coordinator" container and component label.
	if pod.Spec.Containers[0].Name != ControlContainerName {
		t.Fatalf("control container name = %q", pod.Spec.Containers[0].Name)
	}
	if pod.Labels["courier.misospace.dev/component"] != ComponentCoordinator {
		t.Fatalf("control component label = %q", pod.Labels["courier.misospace.dev/component"])
	}
	if pod.Spec.ServiceAccountName != ControlSAName(run.Name) {
		t.Fatalf("control service account = %q", pod.Spec.ServiceAccountName)
	}
	envs := map[string]corev1.EnvVar{}
	for _, env := range pod.Spec.Containers[0].Env {
		envs[env.Name] = env
	}
	for _, forbidden := range []string{EnvForgeAPIToken, EnvGitToken, EnvGitUsername} {
		if env, ok := envs[forbidden]; ok {
			t.Fatalf("control pod carries forge credential env %q = %+v", forbidden, env)
		}
	}
	if envs[EnvWorkerPodUID].Value != "worker-uid" || envs[EnvControlPodUID].Value != "incarnation-1" {
		t.Fatalf("control identity env = %+v %+v", envs[EnvWorkerPodUID], envs[EnvControlPodUID])
	}
	// The projected broker token must be pod-bound with the 600s audience.
	var tokenProjected bool
	for _, v := range pod.Spec.Volumes {
		if v.Projected == nil {
			continue
		}
		for _, src := range v.Projected.Sources {
			tok := src.ServiceAccountToken
			if tok == nil {
				continue
			}
			if tok.Audience != BrokerAudience || tok.ExpirationSeconds == nil || *tok.ExpirationSeconds != ControlTokenSeconds {
				t.Fatalf("control token projection = %+v", tok)
			}
			tokenProjected = true
		}
	}
	if !tokenProjected {
		t.Fatal("control pod must project a broker-audience token")
	}
	mounts := map[string]string{}
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		mounts[m.MountPath] = m.Name
	}
	if _, ok := mounts["/var/run/courier/signing"]; !ok {
		t.Fatal("control pod must mount the signing key")
	}
	if _, ok := mounts["/var/run/courier/broker"]; !ok {
		t.Fatal("control pod must mount the broker CA")
	}
	if envs["COURIER_TERMINATION_FILE"].Value != controlRuntimePath+"/termination" {
		t.Fatal("control termination file path drifts from the operator's contract")
	}
}

func TestBrokerPodProjectionContract(t *testing.T) {
	run := testRun("broker-pod")
	registration := registrationFixture()
	pod, err := BrokerPod(run, "harness:test", registration.Projection(), "sa-uid")
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.ServiceAccountName != BrokerSAName(run.Name) {
		t.Fatalf("broker service account = %q", pod.Spec.ServiceAccountName)
	}
	envs := map[string]corev1.EnvVar{}
	for _, env := range pod.Spec.Containers[0].Env {
		envs[env.Name] = env
	}
	if envs[EnvProviderName].Value != "github" || envs[EnvProviderType].Value != "github" {
		t.Fatalf("provider projection env = %+v", envs)
	}
	for _, name := range []string{EnvForgeAPIToken, EnvGitUsername, EnvGitToken} {
		ref := envs[name].ValueFrom.SecretKeyRef
		if ref == nil || ref.Name != CredentialsSecretName(run.Name) {
			t.Fatalf("env %q must resolve through the per-run credential copy", name)
		}
	}
	// Exactly the fixed key contract of the credential copy.
	if envs[EnvForgeAPIToken].ValueFrom.SecretKeyRef.Key != CredKeyForgeAPIToken ||
		envs[EnvGitToken].ValueFrom.SecretKeyRef.Key != CredKeyGitToken ||
		envs[EnvGitUsername].ValueFrom.SecretKeyRef.Key != CredKeyGitUsername {
		t.Fatal("broker credential env keys drift from the copy contract")
	}
	vols := secretVolumes(pod)
	if vols["tls"] != BrokerTLSSecretName(run.Name) || vols["policy"] != PolicySecretName(run.Name) {
		t.Fatalf("broker volumes = %v", vols)
	}
	if _, ok := vols["api-token"]; !ok {
		t.Fatal("broker must project its own API token")
	}
}

func registrationFixture() *forge.Registration {
	return &forge.Registration{
		Name:     "github",
		Type:     "github",
		Endpoint: "https://api.github.com/",
		Credentials: map[string]forge.CredentialRef{
			forge.PurposeForgeAPI: {SecretName: "src", Key: "token"},
		},
		Serves: []string{"*"},
	}
}

func TestBrokerRoleIsExactlyTheNamedGrants(t *testing.T) {
	run := testRun("rbac")
	role := BrokerRole(run)
	if len(role.Rules) != 4 {
		t.Fatalf("broker role rules = %+v", role.Rules)
	}
	for _, rule := range role.Rules {
		switch {
		case contains(rule.Resources, "coderruns"):
			if len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != run.Name {
				t.Fatalf("coderruns grant must be name-scoped: %+v", rule)
			}
		case contains(rule.Resources, "coderruns/status"):
			if len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != run.Name {
				t.Fatalf("status grant must be name-scoped: %+v", rule)
			}
		case contains(rule.Resources, "pods") && contains(rule.Verbs, "get"):
			// Named pod reads only: the three topology pods.
			want := []string{ControlPodName(run.Name), WorkerPodName(run.Name), BrokerPodName(run.Name)}
			if len(rule.ResourceNames) != 3 {
				t.Fatalf("pod get must be name-scoped: %+v", rule)
			}
			for _, name := range want {
				if !contains(rule.ResourceNames, name) {
					t.Fatalf("pod get missing named pod %q: %+v", name, rule.ResourceNames)
				}
			}
		case contains(rule.Resources, "pods") && contains(rule.Verbs, "list"):
			// The authenticator's exactly-one-live-control scan needs a
			// namespace list; RBAC cannot name-scope it. The run namespace
			// is dedicated, so it only ever sees this run's pods.
			if len(rule.ResourceNames) != 0 {
				t.Fatalf("pod list carries no resourceNames: %+v", rule)
			}
		default:
			t.Fatalf("unexpected rule resource: %+v", rule.Resources)
		}
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func TestServiceAccountsNeverAutomount(t *testing.T) {
	run := testRun("sas")
	for _, sa := range ServiceAccounts(run) {
		if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
			t.Fatalf("service account %s automounts a token", sa.Name)
		}
	}
}

func TestWorkerNetworkPolicyIsolation(t *testing.T) {
	run := testRun("netpol")
	policies := NetworkPolicies(run, nil, nil)
	byComponent := map[string]*networkingv1.NetworkPolicy{}
	for _, p := range policies {
		byComponent[p.Labels["courier.misospace.dev/component"]] = p
	}
	worker := byComponent[ComponentWorker]
	if worker == nil {
		t.Fatal("worker policy missing")
	}
	if len(worker.Spec.Ingress) != 1 || len(worker.Spec.Egress) != 0 {
		t.Fatalf("without a cache pin the worker must allow only control ingress and no egress: %+v", worker.Spec)
	}

	policies = NetworkPolicies(run, &CachePin{IP: "10.96.0.7", Port: 5120}, nil)
	for _, p := range policies {
		if p.Labels["courier.misospace.dev/component"] == ComponentWorker {
			worker = p
		}
	}
	if len(worker.Spec.Egress) != 1 {
		t.Fatalf("worker egress rules = %+v", worker.Spec.Egress)
	}
	if worker.Spec.Egress[0].To[0].IPBlock.CIDR != "10.96.0.7/32" {
		t.Fatalf("cache egress = %+v", worker.Spec.Egress[0].To)
	}

	// IPv6 cache pins use /128.
	policies = NetworkPolicies(run, &CachePin{IP: "fd00::7", Port: 5120}, nil)
	for _, p := range policies {
		if p.Labels["courier.misospace.dev/component"] == ComponentWorker {
			if p.Spec.Egress[0].To[0].IPBlock.CIDR != "fd00::7/128" {
				t.Fatalf("IPv6 cache egress = %+v", p.Spec.Egress[0].To)
			}
		}
	}
}

func TestBrokerTLSIdentity(t *testing.T) {
	tls, err := GenerateBrokerTLS([]string{"r-broker.ns.svc.cluster.local", "r-broker.ns.svc", "r-broker"})
	if err != nil {
		t.Fatal(err)
	}
	caBlock, _ := pem.Decode(tls.CACertPEM)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !caCert.IsCA {
		t.Fatal("CA certificate is not a CA")
	}
	serverBlock, _ := pem.Decode(tls.ServerCertPEM)
	serverCert, err := x509.ParseCertificate(serverBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := serverCert.VerifyHostname("r-broker.ns.svc.cluster.local"); err != nil {
		t.Fatalf("server certificate hostname: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	if _, err := serverCert.Verify(x509.VerifyOptions{DNSName: "r-broker.ns.svc", Roots: roots}); err != nil {
		t.Fatalf("server certificate does not chain to the run CA: %v", err)
	}
	keyBlock, _ := pem.Decode(tls.ServerKeyPEM)
	if _, err := x509.ParseECPrivateKey(keyBlock.Bytes); err != nil {
		t.Fatalf("server key: %v", err)
	}
}

func TestResourceNamesStayUniqueAndStable(t *testing.T) {
	long := "this-run-name-is-far-too-long-for-kubernetes-object-names-" +
		"because-it-keeps-going-and-going-and-going-past-the-limit-abcde"
	first := ControlPodName(long)
	second := ControlPodName(long)
	if first != second {
		t.Fatal("derived names must be stable")
	}
	if len(first) > 253 {
		t.Fatalf("derived name exceeds 253 characters: %d", len(first))
	}
	if ControlPodName("short") != "short-control" {
		t.Fatalf("short name = %q", ControlPodName("short"))
	}
}
