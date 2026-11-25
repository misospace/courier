package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/gitegress"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

func installationTestRun() *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-871", Namespace: "courier-runs"},
		Spec: courierv1alpha1.CoderRunSpec{
			Repo: "acme/widgets",
			Mode: courierv1alpha1.ModeResolveIssue,
			Ref:  7,
			Lane: "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Branch: "courier/acme/widgets/issue-7"},
	}
}

func installationTestLane() *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "courier-system"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "any-model"}},
	}
}

func installationTestConfig() PodConfig {
	return PodConfig{
		Image:               "registry.example/courier-opencode:test",
		WorkspacePath:       "/workspace",
		GitRemoteURL:        "https://github.com/%s.git",
		BaseBranch:          "main",
		GitCredentialSecret: "courier-git",
	}
}

func installationGitConfig() *InstallationGitConfig {
	return &InstallationGitConfig{Owner: "acme", APIEndpoint: "https://api.github.com/", CredentialsSecret: "run-871-provider-creds"}
}

func TestInstallationGitConfigValidate(t *testing.T) {
	if err := (*InstallationGitConfig)(nil).Validate(); err != nil {
		t.Fatalf("nil config must validate: %v", err)
	}
	for _, config := range []*InstallationGitConfig{
		{Owner: "acme", APIEndpoint: "https://api.github.com/", CredentialsSecret: "run-provider-creds"},
		{Owner: "acme", APIEndpoint: "https://github.example/api/v3", CredentialsSecret: "run-provider-creds"},
	} {
		if err := config.Validate(); err != nil {
			t.Fatalf("config %#v rejected: %v", config, err)
		}
	}
	for _, config := range []*InstallationGitConfig{
		{Owner: "", APIEndpoint: "https://api.github.com/", CredentialsSecret: "run-provider-creds"},
		{Owner: " acme", APIEndpoint: "https://api.github.com/", CredentialsSecret: "run-provider-creds"},
		{Owner: "acme\n", APIEndpoint: "https://api.github.com/", CredentialsSecret: "run-provider-creds"},
		{Owner: "acme", APIEndpoint: ""},
		{Owner: "acme", APIEndpoint: "https://api.github.com/"},
		{Owner: "acme", APIEndpoint: "http://api.github.com/", CredentialsSecret: "run-provider-creds"},
		{Owner: "acme", APIEndpoint: "https://user@api.github.com/", CredentialsSecret: "run-provider-creds"},
		{Owner: "acme", APIEndpoint: "https://api.github.com/?x=1", CredentialsSecret: "run-provider-creds"},
		{Owner: "acme", APIEndpoint: "https://api.github.com/", CredentialsSecret: "ns/name"},
	} {
		if err := config.Validate(); err == nil {
			t.Fatalf("config %#v accepted", config)
		}
	}
}

func hasVolumeNamed(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func TestBuildCoordinatorPodGuardRemoteMatchesPinnedGuardIdentity(t *testing.T) {
	config := installationTestConfig()
	config.InstallationGit = installationGitConfig()
	run := installationTestRun()
	pod, err := BuildCoordinatorPod(run, installationTestLane(), config)
	if err != nil {
		t.Fatalf("BuildCoordinatorPod: %v", err)
	}
	remote := envValue(pod.Spec.Containers[0].Env, "COURIER_REPO_URL")
	parsed, err := url.Parse(remote)
	if err != nil {
		t.Fatal(err)
	}
	wantHost := GitGuardServiceDNS(run.Namespace, run.Name) + ":18443"
	if parsed.Scheme != "http" || parsed.Host != wantHost || parsed.EscapedPath() != "/acme/widgets.git" {
		t.Fatalf("COURIER_REPO_URL = %q; want http://%s/acme/widgets.git", remote, wantHost)
	}
	var gotIdentity string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		_, _ = w.Write([]byte("001e# service=git-upload-pack\n0000"))
	}))
	defer upstream.Close()
	guard, err := gitegress.New(gitegress.Config{
		Remotes:     []gitegress.Remote{{URL: upstream.URL + "/acme/widgets.git", Identity: "acme/widgets", Push: true}},
		WorkRef:     "refs/heads/courier/acme/widgets/issue-7",
		Credentials: installationGuardCredentialProvider{identity: &gotIdentity},
	})
	if err != nil {
		t.Fatalf("gitegress.New: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, parsed.EscapedPath()+"/info/refs?service=git-upload-pack", nil)
	response := httptest.NewRecorder()
	guard.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("guard response status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if gotIdentity != "acme/widgets" {
		t.Fatalf("guard credential identity = %q, want acme/widgets", gotIdentity)
	}
}

type installationGuardCredentialProvider struct{ identity *string }

func (p installationGuardCredentialProvider) Credentials(_ context.Context, identity string) (gitegress.Credential, error) {
	*p.identity = identity
	return gitegress.Credential{Username: "user", Token: "token"}, nil
}

func TestBuildCoordinatorPodInstallationGitUsesGuardService(t *testing.T) {
	config := installationTestConfig()
	config.GitHubCredentialSecret = "courier-api"
	config.InstallationGit = installationGitConfig()
	run := installationTestRun()
	pod, err := BuildCoordinatorPod(run, installationTestLane(), config)
	if err != nil {
		t.Fatalf("BuildCoordinatorPod: %v", err)
	}
	if pod.Annotations[GitEgressContractAnnotation] != "2" {
		t.Fatalf("contract annotation=%q", pod.Annotations[GitEgressContractAnnotation])
	}
	for _, container := range append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		if container.Name == GitEgressContainerName {
			t.Fatalf("coordinator still contains guard container")
		}
	}
	want := "http://" + GitGuardServiceDNS(run.Namespace, run.Name) + ":18443/acme/widgets.git"
	if got := envValue(pod.Spec.Containers[0].Env, "COURIER_REPO_URL"); got != want {
		t.Fatalf("remote=%q, want %q", got, want)
	}
	if hasVolumeNamed(pod.Spec.Volumes, gitCredentialsTokenVolumeName) {
		t.Fatal("App-mode coordinator must not carry the guard credential volume")
	}
	if !hasVolumeNamed(pod.Spec.Volumes, observerTokenVolume) {
		t.Fatal("App-mode coordinator pod must provide the observer token volume")
	}
	if len(pod.Spec.InitContainers) == 0 || pod.Spec.InitContainers[0].Name != "workspace-observer-seed" {
		t.Fatalf("observer init containers changed: %+v", pod.Spec.InitContainers)
	}
}

func TestGitGuardPodNameValidityAndUniqueness(t *testing.T) {
	names := []string{
		"simple", "name.with.dots", strings.Repeat("a", 80), "-leading", "trailing-",
		"a." + strings.Repeat("b", 70), strings.Repeat("a", 43) + "." + strings.Repeat("b", 40),
		// A digit-leading name and the run its "r"-anchor would otherwise
		// collide with; the digest the anchored path carries keeps them apart.
		"9digits", "r9digits",
		// The 53/54-character passthrough/digest transition.
		strings.Repeat("a", 53), strings.Repeat("a", 54),
		// Two long names sharing the truncated prefix stay distinct.
		strings.Repeat("a", 44) + "x1", strings.Repeat("a", 44) + "x2",
		// The "run" fallback and the run it must not collide with.
		"", "-", "run",
	}
	seen := map[string]bool{}
	for _, runName := range names {
		name := GitGuardPodName(runName)
		if len(name) > 63 || name == "" || name[0] < 'a' || name[0] > 'z' || name[len(name)-1] == '-' {
			t.Fatalf("GitGuardPodName(%q) = %q, invalid DNS label", runName, name)
		}
		for _, char := range name {
			if !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-') {
				t.Fatalf("GitGuardPodName(%q) = %q, invalid character %q", runName, name, char)
			}
		}
		if seen[name] {
			t.Fatalf("duplicate guard pod name %q", name)
		}
		seen[name] = true
		if got, want := name, GitGuardServiceName(runName); got != want {
			t.Fatalf("GitGuardPodName(%q) = %q, want the guard Service's %q", runName, got, want)
		}
	}
	if got, want := GitGuardPodName("9digits"), "r9digits-dc582376-git-guard"; got != want {
		t.Fatalf("GitGuardPodName(\"9digits\") = %q, want %q", got, want)
	}
	if got, want := GitGuardPodName("simple"), "simple-git-guard"; got != want {
		t.Fatalf("GitGuardPodName(\"simple\") = %q, want %q", got, want)
	}
	if got, want := GitGuardPodName(strings.Repeat("a", 53)), strings.Repeat("a", 53)+"-git-guard"; got != want {
		t.Fatalf("GitGuardPodName(53*a) = %q, want the unchanged passthrough %q", got, want)
	}
}

func TestBuildGitGuardPod(t *testing.T) {
	run := installationTestRun()
	config := installationTestConfig()
	config.InstallationGit = installationGitConfig()
	pod, err := BuildGitGuardPod(run, installationTestLane(), config)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Name != GitGuardPodName(run.Name) || pod.Spec.ServiceAccountName != GitCredentialsServiceAccountName(run.Name) || pod.Spec.RestartPolicy != corev1.RestartPolicyAlways || pod.Spec.Containers[0].RestartPolicy != nil {
		t.Fatalf("guard pod identity/spec: %+v", pod)
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Name != GitEgressContainerName {
		t.Fatalf("guard containers=%+v", pod.Spec.Containers)
	}
	guard := pod.Spec.Containers[0]
	if envValue(guard.Env, EnvGitEgressAddress) != GitGuardListenAddress || envValue(guard.Env, "COURIER_REPO_URL") != "https://github.com/acme/widgets.git" {
		t.Fatalf("guard env=%+v", guard.Env)
	}
	if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].Name != gitCredentialsTokenVolumeName || len(guard.VolumeMounts) != 1 || !guard.VolumeMounts[0].ReadOnly {
		t.Fatalf("guard credential mounts: volumes=%+v mounts=%+v", pod.Spec.Volumes, guard.VolumeMounts)
	}
}

func TestBuildCoordinatorPodStaticModeOmitsInstallationContract(t *testing.T) {
	pod, err := BuildCoordinatorPod(installationTestRun(), installationTestLane(), installationTestConfig())
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	for _, env := range pod.Spec.Containers[0].Env {
		switch env.Name {
		case EnvGitInstallation, EnvInstallationOwner, EnvInstallationEndpoint, EnvInstallationSecret, EnvAPITokenFile, EnvAPICertFile, "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "POD_NAMESPACE":
			t.Fatalf("static pod carries App-mode env %s", env.Name)
		}
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == gitCredentialsTokenVolumeName {
			t.Fatalf("static pod carries the projected token volume")
		}
	}
	for _, containers := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		for _, container := range containers {
			if container.Name == GitEgressContainerName {
				t.Fatalf("static pod carries the git egress guard")
			}
		}
	}
	if _, ok := pod.Annotations[GitEgressContractAnnotation]; ok {
		t.Fatalf("static pod carries the git egress contract annotation")
	}
	if got := envValue(pod.Spec.Containers[0].Env, "COURIER_REPO_URL"); got != "https://github.com/acme/widgets.git" {
		t.Fatalf("static pod remote rewritten: %q", got)
	}
}

func TestLoopbackRemoteTemplate(t *testing.T) {
	for template, want := range map[string]string{
		"https://github.com/%s.git":             "http://git-guard.default.svc.cluster.local:18443/%s.git",
		"https://ghe.example.com/api/%s":        "http://git-guard.default.svc.cluster.local:18443/api/%s",
		"https://github.example.com/owner/%s.g": "http://git-guard.default.svc.cluster.local:18443/owner/%s.g",
	} {
		got, err := guardRemoteTemplate(template, "git-guard.default.svc.cluster.local:18443")
		if err != nil {
			t.Fatalf("%q: %v", template, err)
		}
		if got != want {
			t.Fatalf("%q rewrote to %q, want %q", template, got, want)
		}
	}
	for _, template := range []string{
		"https://github.com/acme/widgets.git",
		"http://github.com/%s.git",
		"git@github.com:%s.git",
		"https://user@github.com/%s.git",
		"https://github.com/%s.git?x=1",
	} {
		if _, err := guardRemoteTemplate(template, "git-guard.default.svc.cluster.local:18443"); err == nil {
			t.Fatalf("%q accepted", template)
		}
	}
	config := installationTestConfig()
	config.InstallationGit = installationGitConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("valid App-mode config rejected: %v", err)
	}
	config.GitRemoteURL = "https://github.com/fixed/widgets.git"
	if err := config.Validate(); err == nil {
		t.Fatalf("App-mode config without the %%s placeholder accepted")
	}
}

func TestCredentialEnvRefsInstallationModeSuppressesGitPair(t *testing.T) {
	config := installationTestConfig()
	refs := CredentialEnvRefs(config, courierv1alpha1.ModeResolveIssue)
	if len(refs) != 3 || refs[2].Name != "GITHUB_TOKEN" || refs[2].Secret != "courier-git" {
		t.Fatalf("static mode must keep the git pair and its GITHUB_TOKEN fallback: %#v", refs)
	}
	config.InstallationGit = installationGitConfig()
	refs = CredentialEnvRefs(config, courierv1alpha1.ModeResolveIssue)
	for _, ref := range refs {
		if ref.Name == "COURIER_GIT_USERNAME" || ref.Name == "COURIER_GIT_TOKEN" {
			t.Fatalf("App mode leaked static git credential ref: %#v", ref)
		}
		if ref.Name == "GITHUB_TOKEN" {
			t.Fatalf("App mode must not fall back GITHUB_TOKEN to the git Secret: %#v", ref)
		}
	}
	config.GitHubCredentialSecret = "courier-api"
	refs = CredentialEnvRefs(config, courierv1alpha1.ModeResolveIssue)
	if len(refs) != 1 || refs[0].Name != "GITHUB_TOKEN" || refs[0].Secret != "courier-api" {
		t.Fatalf("explicit API secret must survive App mode for forge API reads: %#v", refs)
	}
}

func TestBuildCoordinatorPodInstallationGitObserverContract(t *testing.T) {
	config := installationTestConfig()
	config.InstallationGit = installationGitConfig()
	pod, err := BuildCoordinatorPod(installationTestRun(), installationTestLane(), config)
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	if pod.Annotations[ObserverContractAnnotation] != ObserverContractVersion {
		t.Fatalf("opencode pods must carry the observer contract")
	}
	var seed, observer *corev1.Container
	for index := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[index].Name == "workspace-observer-seed" {
			seed = &pod.Spec.InitContainers[index]
		}
	}
	for index := range pod.Spec.Containers {
		if pod.Spec.Containers[index].Name == ObserverContainerName {
			observer = &pod.Spec.Containers[index]
		}
	}
	if seed == nil || observer == nil {
		t.Fatalf("observer containers missing: %#v %#v", pod.Spec.InitContainers, pod.Spec.Containers)
	}
	for _, container := range []*corev1.Container{seed, observer} {
		environment := map[string]corev1.EnvVar{}
		for _, env := range container.Env {
			environment[env.Name] = env
		}
		for _, name := range []string{"COURIER_GIT_USERNAME", "COURIER_GIT_TOKEN"} {
			if env, ok := environment[name]; ok {
				t.Fatalf("App-mode %s must not carry %s: %#v", container.Name, name, env)
			}
		}
		for _, name := range []string{EnvGitInstallation, EnvInstallationOwner, EnvInstallationEndpoint, EnvInstallationSecret, "COURIER_REPO", "COURIER_HEAD_REPO"} {
			if _, ok := environment[name]; ok {
				t.Fatalf("App-mode observer must not carry %s: %#v", name, environment[name])
			}
		}
		wantRemote := "http://" + GitGuardServiceDNS(installationTestRun().Namespace, installationTestRun().Name) + ":18443/acme/widgets.git"
		if environment["COURIER_REPO_URL"].Value != wantRemote || environment["COURIER_BASE_REPO_URL"].Value != wantRemote {
			t.Fatalf("App-mode %s must use guard-routed remotes: %#v %#v", container.Name, environment["COURIER_REPO_URL"], environment["COURIER_BASE_REPO_URL"])
		}
		mounted := false
		for _, mount := range container.VolumeMounts {
			if mount.Name == observerTokenVolume {
				mounted = mount.MountPath == ObserverTokenPath && mount.ReadOnly
			}
		}
		if container == seed && mounted {
			t.Fatal("App-mode seed must not mount the observer token volume")
		}
		if container == observer && !mounted {
			t.Fatal("observer must retain its kube API token mount")
		}
	}
}
