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
	// #902: the model container carries no git credential and no means to
	// read one — no installation contract, no projected-token file, no
	// namespace to call the API with.
	for _, env := range pod.Spec.Containers[0].Env {
		switch env.Name {
		case EnvGitInstallation, EnvInstallationOwner, EnvInstallationEndpoint, EnvInstallationSecret, EnvAPITokenFile, EnvAPICertFile, "POD_NAMESPACE", "GIT_CONFIG_COUNT":
			t.Fatalf("App-mode coordinator must not carry %s: the guard is the only credential reader", env.Name)
		}
	}
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == gitCredentialsTokenVolumeName {
			t.Fatalf("App-mode coordinator must not mount the projected token: %#v", mount)
		}
	}
	if got := environment["COURIER_REPO_URL"].Value; got != "http://"+GitEgressAddress+"/acme/widgets.git" {
		t.Fatalf("COURIER_REPO_URL = %q, want the guard loopback remote", got)
	}
	if got := environment["COURIER_BASE_REPO_URL"].Value; got != "http://"+GitEgressAddress+"/acme/widgets.git" {
		t.Fatalf("COURIER_BASE_REPO_URL = %q, want the guard loopback remote", got)
	}
	if pod.Annotations[GitEgressContractAnnotation] != GitEgressContractVersion {
		t.Fatalf("App-mode pod must carry the git egress contract annotation")
	}
	var guard *corev1.Container
	for index := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[index].Name == GitEgressContainerName {
			guard = &pod.Spec.InitContainers[index]
		}
	}
	if guard == nil {
		t.Fatalf("git egress guard init container missing: %#v", pod.Spec.InitContainers)
	}
	if pod.Spec.InitContainers[0].Name != GitEgressContainerName {
		t.Fatalf("the guard must be the first init container so it starts before every other container: %#v", pod.Spec.InitContainers)
	}
	for index := range pod.Spec.Containers {
		if pod.Spec.Containers[index].Name == GitEgressContainerName {
			t.Fatalf("git egress guard must be a native sidecar, not an app container")
		}
	}
	if guard.RestartPolicy == nil || *guard.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatalf("guard restart policy = %v, want Always so the kubelet stops it with the app containers", guard.RestartPolicy)
	}
	guardEnv := map[string]corev1.EnvVar{}
	for _, env := range guard.Env {
		guardEnv[env.Name] = env
	}
	for name, want := range map[string]string{
		EnvGitEgressAddress:     GitEgressAddress,
		EnvInstallationOwner:    "acme",
		EnvInstallationEndpoint: "https://api.github.com/",
		EnvInstallationSecret:   "run-871-provider-creds",
		EnvAPITokenFile:         GitCredentialsTokenPath + "/token",
		EnvAPICertFile:          GitCredentialsTokenPath + "/ca.crt",
		"COURIER_REPO":          "acme/widgets",
		"COURIER_REPO_URL":      "https://github.com/acme/widgets.git",
		"COURIER_BASE_REPO_URL": "https://github.com/acme/widgets.git",
		"COURIER_BRANCH":        "courier/acme/widgets/issue-7",
	} {
		if got := guardEnv[name].Value; got != want {
			t.Fatalf("guard %s = %q, want %q", name, got, want)
		}
	}
	if _, ok := guardEnv["COURIER_HEAD_REPO"]; !ok {
		t.Fatalf("guard env must carry the head identity even when empty")
	}
	namespace, ok := guardEnv["POD_NAMESPACE"]
	if !ok || namespace.ValueFrom == nil || namespace.ValueFrom.FieldRef == nil || namespace.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
		t.Fatalf("guard POD_NAMESPACE = %#v", namespace)
	}
	var volumeFound, guardMount bool
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
	for _, mount := range guard.VolumeMounts {
		if mount.Name == gitCredentialsTokenVolumeName {
			guardMount = mount.MountPath == GitCredentialsTokenPath && mount.ReadOnly
		}
	}
	if !guardMount {
		t.Fatalf("guard token mount missing or wrong: %#v", guard.VolumeMounts)
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
		"https://github.com/%s.git":             "http://" + GitEgressAddress + "/%s.git",
		"https://ghe.example.com/api/%s":        "http://" + GitEgressAddress + "/api/%s",
		"https://github.example.com/owner/%s.g": "http://" + GitEgressAddress + "/owner/%s.g",
	} {
		got, err := loopbackRemoteTemplate(template, GitEgressAddress)
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
		if _, err := loopbackRemoteTemplate(template, GitEgressAddress); err == nil {
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
		if environment["COURIER_REPO_URL"].Value != "https://github.com/acme/widgets.git" ||
			environment["COURIER_BASE_REPO_URL"].Value != "https://github.com/acme/widgets.git" {
			t.Fatalf("App-mode %s must keep the absolute forge remotes, not the guard loopback: %#v %#v",
				container.Name, environment["COURIER_REPO_URL"], environment["COURIER_BASE_REPO_URL"])
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
