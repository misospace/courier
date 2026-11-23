package executor

import (
	"testing"

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

func TestBuildCoordinatorPodInstallationGitReplacesStaticPair(t *testing.T) {
	config := installationTestConfig()
	config.GitHubCredentialSecret = "courier-api"
	config.InstallationGit = installationGitConfig()
	pod, err := BuildCoordinatorPod(installationTestRun(), installationTestLane(), config)
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	environment := map[string]corev1.EnvVar{}
	for _, env := range pod.Spec.Containers[0].Env {
		environment[env.Name] = env
	}
	for _, name := range []string{"COURIER_GIT_USERNAME", "COURIER_GIT_TOKEN"} {
		if env, ok := environment[name]; ok {
			t.Fatalf("App-mode pod must not carry %s: %#v", name, env)
		}
	}
	github, ok := environment["GITHUB_TOKEN"]
	if !ok || github.ValueFrom == nil || github.ValueFrom.SecretKeyRef == nil ||
		github.ValueFrom.SecretKeyRef.Name != "courier-api" {
		t.Fatalf("the explicitly configured API Secret must still wire GITHUB_TOKEN, and never from the git Secret: %#v", github)
	}
	if got := environment[EnvGitInstallation].Value; got != "1" {
		t.Fatalf("%s = %q, want 1", EnvGitInstallation, got)
	}
	if got := environment[EnvInstallationOwner].Value; got != "acme" {
		t.Fatalf("%s = %q, want acme", EnvInstallationOwner, got)
	}
	if got := environment[EnvInstallationEndpoint].Value; got != "https://api.github.com/" {
		t.Fatalf("%s = %q", EnvInstallationEndpoint, got)
	}
	if got := environment[EnvInstallationSecret].Value; got != "run-871-provider-creds" {
		t.Fatalf("%s = %q", EnvInstallationSecret, got)
	}
	if got := environment[EnvAPITokenFile].Value; got != GitCredentialsTokenPath+"/token" {
		t.Fatalf("%s = %q", EnvAPITokenFile, got)
	}
	if got := environment[EnvAPICertFile].Value; got != GitCredentialsTokenPath+"/ca.crt" {
		t.Fatalf("%s = %q", EnvAPICertFile, got)
	}
	if environment["GIT_CONFIG_KEY_0"].Value != "credential.useHttpPath" || environment["GIT_CONFIG_VALUE_0"].Value != "true" {
		t.Fatalf("credential.useHttpPath not installed for path-bound prompts: %#v %#v", environment["GIT_CONFIG_KEY_0"], environment["GIT_CONFIG_VALUE_0"])
	}
	if environment["GIT_CONFIG_COUNT"].Value != "1" {
		t.Fatalf("GIT_CONFIG_COUNT = %q", environment["GIT_CONFIG_COUNT"].Value)
	}
	namespace, ok := environment["POD_NAMESPACE"]
	if !ok || namespace.ValueFrom == nil || namespace.ValueFrom.FieldRef == nil || namespace.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
		t.Fatalf("POD_NAMESPACE = %#v", namespace)
	}
	var volumeFound bool
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != gitCredentialsTokenVolumeName {
			continue
		}
		volumeFound = true
		projected := volume.Projected
		if projected == nil || len(projected.Sources) != 2 ||
			projected.Sources[0].ServiceAccountToken == nil || projected.Sources[0].ServiceAccountToken.Path != "token" ||
			projected.Sources[1].ConfigMap == nil || projected.Sources[1].ConfigMap.Name != observerCAName {
			t.Fatalf("token projection = %#v", projected)
		}
	}
	if !volumeFound {
		t.Fatalf("projected token volume %q missing", gitCredentialsTokenVolumeName)
	}
	var mountFound bool
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == gitCredentialsTokenVolumeName {
			mountFound = mount.MountPath == GitCredentialsTokenPath && mount.ReadOnly
		}
	}
	if !mountFound {
		t.Fatalf("coordinator token mount missing or wrong: %#v", pod.Spec.Containers[0].VolumeMounts)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatalf("pod-level automount must stay false")
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
		if environment[EnvGitInstallation].Value != "1" ||
			environment[EnvInstallationOwner].Value != "acme" ||
			environment[EnvInstallationEndpoint].Value != "https://api.github.com/" ||
			environment[EnvInstallationSecret].Value != "run-871-provider-creds" ||
			environment["COURIER_REPO"].Value != "acme/widgets" {
			t.Fatalf("App-mode observer env incomplete: %#v", container.Env)
		}
		if _, ok := environment["COURIER_HEAD_REPO"]; !ok {
			t.Fatalf("App-mode observer env must carry the head identity for fork bindings, even when empty: %#v", container.Env)
		}
		var mounted bool
		for _, mount := range container.VolumeMounts {
			if mount.Name == observerTokenVolume {
				mounted = mount.MountPath == ObserverTokenPath && mount.ReadOnly
			}
		}
		if !mounted {
			t.Fatalf("App-mode %s must mount the observer token volume", container.Name)
		}
	}
}
