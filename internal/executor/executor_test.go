package executor

import (
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

func TestOpenCodeCommandInjectsGoalModelAndFraming(t *testing.T) {
	invocation := Invocation{
		Goal:    "Open a PR for issue #7.",
		Model:   "litellm/qwen",
		Framing: "single GPU; keep parallelism modest",
	}

	command := (OpenCode{Binary: "/usr/local/bin/opencode", Format: "json"}).Command(invocation)
	if command.Binary != "/usr/local/bin/opencode" {
		t.Fatalf("command binary = %q, want configured binary", command.Binary)
	}
	if got, want := command.Args[:4], []string{"run", "--model", "litellm/qwen", "--format"}; !sameStrings(got, want) {
		t.Fatalf("command prefix = %#v, want %#v", got, want)
	}
	if got := command.Args[5]; !strings.Contains(got, invocation.Goal) || !strings.Contains(got, invocation.Framing) {
		t.Fatalf("command prompt = %q, want goal and framing", got)
	}
}

func TestOpenCodeResultMapsEveryTermination(t *testing.T) {
	runtime := DefaultOpenCode()
	for _, test := range []struct {
		name     string
		exitCode int
		err      error
		phase    TerminalPhase
	}{
		{name: "success", exitCode: OpenCodeExitSuccess, phase: TerminalAwaitingReview},
		{name: "needs human", exitCode: OpenCodeExitNeedsHuman, phase: TerminalNeedsHuman},
		{name: "other exit", exitCode: 1, phase: TerminalFailed},
		{name: "process error", exitCode: 0, err: errors.New("backend unavailable"), phase: TerminalFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := runtime.Result(test.exitCode, test.err)
			if outcome.Phase != test.phase {
				t.Fatalf("phase = %q, want %q", outcome.Phase, test.phase)
			}
			if outcome.Reason == "" {
				t.Fatal("result reason is empty")
			}
		})
	}
}

func TestBuildCoordinatorPodInjectsRunContextAndEphemeralWorkspace(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-7",
			Namespace: "courier-system",
			UID:       "run-uid",
		},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:  courierv1alpha1.ModeResolveIssue,
			Repo:  "acme/widgets",
			Ref:   7,
			Lane:  "local",
			Debug: true,
		},
		Status: courierv1alpha1.CoderRunStatus{Branch: "courier/acme/widgets/issue-7"},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "courier-system"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles:   map[string]string{"coordinator": "litellm/qwen"},
			Framing: "single GPU; keep parallelism modest",
		},
	}

	pod, err := BuildCoordinatorPod(run, lane, PodConfig{
		Image:              "registry.example/courier-opencode:test",
		WorkspacePath:      "/workspace",
		ServiceAccountName: "coordinator",
		RuntimeClassName:   "kata",
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	if pod.Name != "run-7-coordinator" || pod.Namespace != run.Namespace {
		t.Fatalf("pod identity = %s/%s, want %s/%s", pod.Namespace, pod.Name, run.Namespace, "run-7-coordinator")
	}
	if pod.Labels[LabelRun] != run.Name || pod.Labels[LabelComponent] != "coordinator" || pod.Labels[LabelExecutor] != "opencode" {
		t.Fatalf("pod labels = %#v, missing run/component/executor labels", pod.Labels)
	}
	if pod.Spec.ServiceAccountName != "coordinator" || pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "kata" {
		t.Fatalf("runtime settings = service account %q, runtime class %v", pod.Spec.ServiceAccountName, pod.Spec.RuntimeClassName)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("coordinator pod automounts a Kubernetes service-account token")
	}
	if pod.Spec.RestartPolicy != "Never" {
		t.Fatalf("restart policy = %q, want Never", pod.Spec.RestartPolicy)
	}
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
		t.Fatal("pod does not require a non-root security context")
	}
	container := pod.Spec.Containers[0]
	if container.Image != "registry.example/courier-opencode:test" || container.WorkingDir != "/workspace" {
		t.Fatalf("container runtime = image %q, working dir %q", container.Image, container.WorkingDir)
	}
	if len(container.Command) != 1 || container.Command[0] != "/usr/local/bin/courier-executor" || len(container.Args) != 0 {
		t.Fatalf("container command = %v %v, want executable bootstrap without shell args", container.Command, container.Args)
	}
	if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("container allows privilege escalation")
	}
	if len(container.SecurityContext.Capabilities.Drop) != 1 || container.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("dropped capabilities = %#v, want ALL", container.SecurityContext.Capabilities.Drop)
	}
	if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].EmptyDir == nil {
		t.Fatal("workspace is not an EmptyDir volume")
	}
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].MountPath != "/workspace" {
		t.Fatal("workspace EmptyDir is not mounted at /workspace")
	}
	env := make(map[string]string, len(container.Env))
	for _, value := range container.Env {
		env[value.Name] = value.Value
	}
	for key, want := range map[string]string{
		"COURIER_GOAL":        "Open a PR to address issue #7. Make sure CI is green and it's ready for review, and delegate as much as possible to keep your context clean.",
		"COURIER_MODEL":       "litellm/qwen",
		"COURIER_FRAMING":     "single GPU; keep parallelism modest",
		"COURIER_LOG_LEVEL":   "debug",
		"COURIER_REPO":        "acme/widgets",
		"COURIER_BRANCH":      "courier/acme/widgets/issue-7",
		"COURIER_WORKSPACE":   "/workspace",
		"COURIER_BASE":        "main",
		"GIT_AUTHOR_NAME":     "Courier",
		"GIT_AUTHOR_EMAIL":    "courier@localhost",
		"GIT_COMMITTER_NAME":  "Courier",
		"GIT_COMMITTER_EMAIL": "courier@localhost",
	} {
		if env[key] != want {
			t.Fatalf("env %s = %q, want %q", key, env[key], want)
		}
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != run.Name {
		t.Fatalf("owner references = %#v, want CoderRun owner", pod.OwnerReferences)
	}
}

func TestBuildCoordinatorPodWiresGitSecretWithoutEmbeddingCredentials(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-secret", Namespace: "courier-system"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  7,
			Lane: "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Branch: "courier/acme/widgets/issue-7"},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "courier-system"},
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "any-model"}},
	}
	pod, err := BuildCoordinatorPod(run, lane, PodConfig{
		Image:               "registry.example/courier-opencode:test",
		WorkspacePath:       "/workspace",
		GitRemoteURL:        "https://git.example/%s.git",
		BaseBranch:          "trunk",
		GitCredentialSecret: "courier-git",
		EnvironmentSecret:   "courier-model",
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	var username, token, githubToken *corev1.EnvVar
	for index := range pod.Spec.Containers[0].Env {
		env := &pod.Spec.Containers[0].Env[index]
		switch env.Name {
		case "COURIER_GIT_USERNAME":
			username = env
		case "COURIER_GIT_TOKEN":
			token = env
		case "GITHUB_TOKEN":
			githubToken = env
		case "COURIER_REPO_URL":
			if env.Value != "https://git.example/acme/widgets.git" {
				t.Fatalf("repository URL = %q", env.Value)
			}
		}
	}
	if username == nil || token == nil || username.Value != "" || token.Value != "" {
		t.Fatalf("credentials were embedded instead of secret references: username=%#v token=%#v", username, token)
	}
	if username.ValueFrom == nil || username.ValueFrom.SecretKeyRef == nil || username.ValueFrom.SecretKeyRef.Name != "courier-git" || username.ValueFrom.SecretKeyRef.Key != "username" {
		t.Fatalf("username secret reference = %#v", username.ValueFrom)
	}
	if token.ValueFrom == nil || token.ValueFrom.SecretKeyRef == nil || token.ValueFrom.SecretKeyRef.Name != "courier-git" || token.ValueFrom.SecretKeyRef.Key != "token" {
		t.Fatalf("token secret reference = %#v", token.ValueFrom)
	}
	if githubToken == nil || githubToken.ValueFrom == nil || githubToken.ValueFrom.SecretKeyRef == nil || githubToken.ValueFrom.SecretKeyRef.Name != "courier-git" || githubToken.ValueFrom.SecretKeyRef.Key != "token" {
		t.Fatalf("GitHub tool token reference = %#v", githubToken)
	}
	if envFrom := pod.Spec.Containers[0].EnvFrom; len(envFrom) != 1 || envFrom[0].SecretRef == nil || envFrom[0].SecretRef.Name != "courier-model" {
		t.Fatalf("model environment secret = %#v", envFrom)
	}
}

func TestRepositoryTemplateEscapesPathComponents(t *testing.T) {
	got := escapedRepositoryPath("acme/widgets?read=all")
	if got != "acme/widgets%3Fread=all" {
		t.Fatalf("escaped repository path = %q, want query-safe path", got)
	}
	if got := escapedRepositoryPath("acme/../secrets"); got != "acme/..%2Fsecrets" {
		t.Fatalf("escaped repository path = %q, want slash-safe path", got)
	}
}

func TestPodNameKeepsLongRunNamesUnique(t *testing.T) {
	first := strings.Repeat("a", 250) + "-one"
	second := strings.Repeat("a", 250) + "-two"
	name1 := podName(first)
	name2 := podName(second)
	if len(name1) > 253 || len(name2) > 253 {
		t.Fatalf("pod names exceed Kubernetes limit: %d, %d", len(name1), len(name2))
	}
	if name1 == name2 {
		t.Fatalf("long run names collided on pod name %q", name1)
	}
}

func TestGoalRejectsInvalidRuns(t *testing.T) {
	if _, err := Goal(nil); !errors.Is(err, ErrNilRun) {
		t.Fatalf("Goal(nil) error = %v, want %v", err, ErrNilRun)
	}
	run := &courierv1alpha1.CoderRun{Spec: courierv1alpha1.CoderRunSpec{Ref: 0}}
	if _, err := Goal(run); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("Goal(zero ref) error = %v, want %v", err, ErrInvalidReference)
	}
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
