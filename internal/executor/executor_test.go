package executor

import (
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
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

func TestOpenCodeCommandIncludesConfiguredAgent(t *testing.T) {
	invocation := Invocation{Goal: "goal", Model: "model"}
	wantPrompt := "goal\n\nUse the Courier scratch directory at /var/tmp/courier-scratch (also set as TMPDIR) for all temporary work, and tell any delegated sub-agents to do the same."
	for _, test := range []struct {
		name  string
		agent string
		want  []string
	}{
		{name: "trimmed lead", agent: " lead ", want: []string{"run", "--model", "model", "--agent", "lead", wantPrompt}},
		{name: "arbitrary architect", agent: "architect", want: []string{"run", "--model", "model", "--agent", "architect", wantPrompt}},
		{name: "empty", want: []string{"run", "--model", "model", wantPrompt}},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := (OpenCode{Binary: "opencode", Agent: test.agent}).Command(invocation)
			if !sameStrings(command.Args, test.want) {
				t.Fatalf("command args = %#v, want %#v", command.Args, test.want)
			}
		})
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
		{name: "success", exitCode: OpenCodeExitSuccess, phase: TerminalVerifying},
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
	if pod.Spec.SecurityContext.RunAsUser == nil || *pod.Spec.SecurityContext.RunAsUser != CoordinatorUID {
		t.Fatalf("pod runAsUser = %v, want %d", pod.Spec.SecurityContext.RunAsUser, CoordinatorUID)
	}
	if pod.Spec.SecurityContext.RunAsGroup == nil || *pod.Spec.SecurityContext.RunAsGroup != CoordinatorGID {
		t.Fatalf("pod runAsGroup = %v, want %d", pod.Spec.SecurityContext.RunAsGroup, CoordinatorGID)
	}
	if pod.Spec.SecurityContext.FSGroup == nil || *pod.Spec.SecurityContext.FSGroup != CoordinatorGID {
		t.Fatalf("pod fsGroup = %v, want %d for the emptyDir workspace", pod.Spec.SecurityContext.FSGroup, CoordinatorGID)
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
	foundWorkspaceVolume := false
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != "workspace" {
			continue
		}
		if volume.EmptyDir == nil {
			t.Fatal("workspace is not an EmptyDir volume")
		}
		foundWorkspaceVolume = true
		break
	}
	if !foundWorkspaceVolume {
		t.Fatal("workspace EmptyDir volume is missing")
	}
	foundWorkspaceMount := false
	for _, mount := range container.VolumeMounts {
		if mount.Name == "workspace" && mount.MountPath == "/workspace" {
			foundWorkspaceMount = true
			break
		}
	}
	if !foundWorkspaceMount {
		t.Fatal("workspace EmptyDir is not mounted at /workspace")
	}
	foundRuntimeVolume := false
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "runtime" && volume.EmptyDir != nil {
			foundRuntimeVolume = true
		}
	}
	foundRuntimeMount := false
	for _, mount := range container.VolumeMounts {
		if mount.Name == "runtime" && mount.MountPath == runtimePath {
			foundRuntimeMount = true
		}
	}
	if !foundRuntimeVolume || !foundRuntimeMount {
		t.Fatal("termination runtime volume is not mounted outside the checkout")
	}
	env := make(map[string]string, len(container.Env))
	for _, value := range container.Env {
		env[value.Name] = value.Value
	}
	for key, want := range map[string]string{
		"COURIER_MODEL":            "litellm/qwen",
		"COURIER_OPENCODE_AGENT":   "",
		"COURIER_FRAMING":          "single GPU; keep parallelism modest",
		"COURIER_LOG_LEVEL":        "debug",
		"COURIER_REPO":             "acme/widgets",
		"COURIER_BRANCH":           "courier/acme/widgets/issue-7",
		"COURIER_WORKSPACE":        "/workspace",
		"COURIER_TERMINATION_FILE": runtimePath + "/termination",
		"COURIER_BASE":             "main",
		"GIT_AUTHOR_NAME":          "Courier",
		"GIT_AUTHOR_EMAIL":         "courier@localhost",
		"GIT_COMMITTER_NAME":       "Courier",
		"GIT_COMMITTER_EMAIL":      "courier@localhost",
	} {
		if env[key] != want {
			t.Fatalf("env %s = %q, want %q", key, env[key], want)
		}
	}
	wantGoal, err := Goal(run)
	if err != nil {
		t.Fatalf("Goal() error = %v", err)
	}
	if env["COURIER_GOAL"] != wantGoal {
		t.Fatalf("COURIER_GOAL = %q, want %q (the Goal for the same run)", env["COURIER_GOAL"], wantGoal)
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != run.Name {
		t.Fatalf("owner references = %#v, want CoderRun owner", pod.OwnerReferences)
	}
}

func TestBuildCoordinatorPodUsesLaneRuntimeImage(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-runtime-image", Namespace: "courier-system"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  7,
			Lane: "go-dogfood",
		},
		Status: courierv1alpha1.CoderRunStatus{Branch: "courier/resolve-issue/acme-widgets/7"},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "go-dogfood", Namespace: "courier-system"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles:        map[string]string{"coordinator": "any-model"},
			RuntimeImage: "registry.example/courier-go:test",
		},
	}
	pod, err := BuildCoordinatorPod(run, lane, PodConfig{Image: "registry.example/courier-opencode:test"})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	if got := pod.Spec.Containers[0].Image; got != lane.Spec.RuntimeImage {
		t.Fatalf("coordinator image = %q, want lane runtime image %q", got, lane.Spec.RuntimeImage)
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

func TestBuildCoordinatorPodWiresDistinctGitHubSecret(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-distinct-secret", Namespace: "courier-system"},
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
		Image:                  "registry.example/courier-opencode:test",
		WorkspacePath:          "/workspace",
		GitCredentialSecret:    "courier-git",
		GitTokenKey:            "git-token",
		GitHubCredentialSecret: "courier-github-api",
		GitHubTokenKey:         "api-token",
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	refs := make(map[string]*corev1.SecretKeySelector)
	for index := range pod.Spec.Containers[0].Env {
		env := &pod.Spec.Containers[0].Env[index]
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			refs[env.Name] = env.ValueFrom.SecretKeyRef
		}
	}
	if ref := refs["COURIER_GIT_TOKEN"]; ref == nil || ref.Name != "courier-git" || ref.Key != "git-token" {
		t.Fatalf("git token reference = %#v", ref)
	}
	if ref := refs["GITHUB_TOKEN"]; ref == nil || ref.Name != "courier-github-api" || ref.Key != "api-token" {
		t.Fatalf("GitHub token reference = %#v", ref)
	}
}

func TestBuildCoordinatorPodFallsBackToGitTokenKeyForGitHubSecret(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-github-key-fallback", Namespace: "courier-system"},
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
		Image:                  "registry.example/courier-opencode:test",
		WorkspacePath:          "/workspace",
		GitCredentialSecret:    "courier-git",
		GitTokenKey:            "git-token",
		GitHubCredentialSecret: "courier-github-api",
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == "GITHUB_TOKEN" {
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil || env.ValueFrom.SecretKeyRef.Name != "courier-github-api" || env.ValueFrom.SecretKeyRef.Key != "git-token" {
				t.Fatalf("GitHub token reference = %#v", env.ValueFrom)
			}
			return
		}
	}
	t.Fatal("GITHUB_TOKEN environment variable is missing")
}

func TestBuildCoordinatorPodWiresAPIOnlySecret(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-api-only-secret", Namespace: "courier-system"},
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
		Image:                  "registry.example/courier-opencode:test",
		WorkspacePath:          "/workspace",
		GitHubCredentialSecret: "courier-github-api",
		GitHubTokenKey:         "api-token",
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	foundGitHub := false
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == "COURIER_GIT_USERNAME" || env.Name == "COURIER_GIT_TOKEN" {
			t.Fatalf("git credential environment variable %q was unexpectedly wired", env.Name)
		}
		if env.Name == "GITHUB_TOKEN" {
			foundGitHub = true
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil || env.ValueFrom.SecretKeyRef.Name != "courier-github-api" || env.ValueFrom.SecretKeyRef.Key != "api-token" {
				t.Fatalf("GitHub token reference = %#v", env.ValueFrom)
			}
		}
	}
	if !foundGitHub {
		t.Fatal("GITHUB_TOKEN environment variable is missing")
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

// forgeCLINames matches a forge name or forge-specific CLI as a whole word so
// ordinary words like "through" (which contains "gh") do not trip it.
var forgeCLINames = regexp.MustCompile(`(?i)\b(gh|glab|tea|gitea|forgejo|hub|github)\b`)

func TestGoalCarriesCoordinatorCompletionContract(t *testing.T) {
	tests := []struct {
		name    string
		mode    courierv1alpha1.Mode
		opening string
		extra   []string
	}{
		{name: "resolve-issue", mode: courierv1alpha1.ModeResolveIssue, opening: "Open a PR", extra: []string{"drive it to a review-ready state with CI green"}},
		{name: "fix-pr", mode: courierv1alpha1.ModeFixPR, opening: "Take over PR", extra: []string{"pull request state", "CI/checks", "review feedback", "determine what's blocking it", "return it to a review-ready state"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &courierv1alpha1.CoderRun{
				Spec: courierv1alpha1.CoderRunSpec{
					Mode: test.mode,
					Ref:  7,
				},
			}
			goal, err := Goal(run)
			if err != nil {
				t.Fatalf("Goal() error = %v", err)
			}
			for _, fragment := range []string{
				"configured forge capability",
				"not a forge-specific CLI",
				"you own completion",
				"push the branch",
				"open or update the pull request",
				"never stop at a local commit or branch when a pull request is required",
				"7",
				test.opening,
			} {
				if !strings.Contains(goal, fragment) {
					t.Fatalf("goal %q missing fragment %q", goal, fragment)
				}
			}
			for _, fragment := range test.extra {
				if !strings.Contains(goal, fragment) {
					t.Fatalf("goal %q missing fragment %q", goal, fragment)
				}
			}
			if test.mode == courierv1alpha1.ModeResolveIssue && strings.Contains(goal, "determine what's blocking it") {
				t.Fatalf("resolve-issue goal must not carry fix-pr blocker-diagnosis phrasing: %q", goal)
			}
			if strings.Contains(goal, "delegate as much as possible") {
				t.Fatalf("goal %q retains the old delegation phrasing", goal)
			}
			if forgeCLINames.MatchString(goal) {
				t.Fatalf("goal %q references a forge-specific CLI", goal)
			}
		})
	}
}

func TestBuildCoordinatorPodWiresRolesAndMCPConfig(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-mcp", Namespace: "courier-system"},
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
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles: map[string]string{
				"coordinator":  "litellm/coordinator",
				"coder":        "litellm/coder",
				"reviewer":     "litellm/reviewer",
				"investigator": "vendor/investigator",
			},
		},
	}
	pod, err := BuildCoordinatorPod(run, lane, PodConfig{
		Image:                  "registry.example/courier-opencode:test",
		WorkspacePath:          "/workspace",
		GitHubCredentialSecret: "courier-github-api",
		GitHubTokenKey:         "api-token",
		GitHubMCPURL:           "https://github-mcp.example/mcp",
		Context7MCPURL:         "https://context7.example/mcp",
		MetricsMCPURL:          "https://metrics.example/mcp",
		OpenCode:               OpenCode{Binary: "opencode", Format: "json", Agent: "architect"},
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	container := pod.Spec.Containers[0]

	env := make(map[string]string, len(container.Env))
	for _, value := range container.Env {
		if value.ValueFrom == nil {
			env[value.Name] = value.Value
		}
	}
	if env["COURIER_MODEL"] != "litellm/coordinator" {
		t.Fatalf("COURIER_MODEL = %q, want the coordinator model", env["COURIER_MODEL"])
	}
	if env["COURIER_OPENCODE_AGENT"] != "architect" {
		t.Fatalf("COURIER_OPENCODE_AGENT = %q, want architect independent of lane roles", env["COURIER_OPENCODE_AGENT"])
	}

	var roles map[string]string
	if err := json.Unmarshal([]byte(env["COURIER_ROLES_JSON"]), &roles); err != nil {
		t.Fatalf("COURIER_ROLES_JSON = %q, not valid JSON: %v", env["COURIER_ROLES_JSON"], err)
	}
	wantRoles := map[string]string{
		"coordinator":  "litellm/coordinator",
		"coder":        "litellm/coder",
		"reviewer":     "litellm/reviewer",
		"investigator": "vendor/investigator",
	}
	if !reflect.DeepEqual(roles, wantRoles) {
		t.Fatalf("COURIER_ROLES_JSON = %#v, want %#v", roles, wantRoles)
	}

	if env["OPENCODE_CONFIG"] != opencodeConfigMountPath+"/"+opencodeConfigFilename {
		t.Fatalf("OPENCODE_CONFIG = %q, want %q", env["OPENCODE_CONFIG"], opencodeConfigMountPath+"/"+opencodeConfigFilename)
	}

	var githubToken *corev1.EnvVar
	for index := range container.Env {
		if container.Env[index].Name == "GITHUB_TOKEN" {
			githubToken = &container.Env[index]
		}
	}
	if githubToken == nil || githubToken.ValueFrom == nil || githubToken.ValueFrom.SecretKeyRef == nil ||
		githubToken.ValueFrom.SecretKeyRef.Name != "courier-github-api" || githubToken.ValueFrom.SecretKeyRef.Key != "api-token" {
		t.Fatalf("GITHUB_TOKEN = %#v, want a secret reference without an embedded literal", githubToken)
	}

	annotation, ok := pod.Annotations[opencodeConfigAnnotation]
	if !ok {
		t.Fatal("opencode config annotation is missing")
	}
	var cfg openCodeConfig
	if err := json.Unmarshal([]byte(annotation), &cfg); err != nil {
		t.Fatalf("opencode config annotation = %q, not valid JSON: %v", annotation, err)
	}

	wantPermission := map[string]any{
		"github_merge_pull_request": "deny",
		"github_merge*":             "deny",
		"external_directory": map[string]any{
			"/var/tmp/courier-scratch/**": "allow",
		},
	}
	wantAgents := map[string]openCodeAgent{
		"coordinator":  {Mode: "all", Model: "litellm/coordinator"},
		"coder":        {Mode: "all", Model: "litellm/coder"},
		"reviewer":     {Mode: "all", Model: "litellm/reviewer"},
		"investigator": {Mode: "all", Model: "vendor/investigator"},
	}
	if !reflect.DeepEqual(cfg.Agents, wantAgents) {
		t.Fatalf("opencode config agents = %#v, want %#v", cfg.Agents, wantAgents)
	}
	if !reflect.DeepEqual(cfg.Permission, wantPermission) {
		t.Fatalf("opencode config permission = %#v, want %#v", cfg.Permission, wantPermission)
	}

	wantMCP := map[string]openCodeMCP{
		"github": {
			Type:    "remote",
			URL:     "https://github-mcp.example/mcp",
			Enabled: true,
			OAuth:   boolPtr(false),
			Headers: map[string]string{"Authorization": "Bearer {env:GITHUB_TOKEN}"},
		},
		"context7": {
			Type:    "remote",
			URL:     "https://context7.example/mcp",
			Enabled: true,
			OAuth:   boolPtr(false),
			Headers: map[string]string{"CONTEXT7_API_KEY": "{env:CONTEXT7_API_KEY}"},
		},
		"metrics": {
			Type:    "remote",
			URL:     "https://metrics.example/mcp",
			Enabled: true,
		},
	}
	if !reflect.DeepEqual(cfg.MCP, wantMCP) {
		t.Fatalf("opencode config mcp = %#v, want %#v", cfg.MCP, wantMCP)
	}
	if !strings.Contains(annotation, "{env:GITHUB_TOKEN}") || !strings.Contains(annotation, "{env:CONTEXT7_API_KEY}") {
		t.Fatal("opencode config does not reference credentials by environment placeholder")
	}

	var rawConfig map[string]json.RawMessage
	if err := json.Unmarshal([]byte(annotation), &rawConfig); err != nil {
		t.Fatalf("raw opencode config = %q, not valid JSON: %v", annotation, err)
	}
	var rawAgents map[string]json.RawMessage
	if err := json.Unmarshal(rawConfig["agent"], &rawAgents); err != nil {
		t.Fatalf("raw opencode agents = %q, not valid JSON: %v", rawConfig["agent"], err)
	}
	for role, rawAgent := range rawAgents {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(rawAgent, &fields); err != nil {
			t.Fatalf("raw opencode agent %q = %q, not valid JSON: %v", role, rawAgent, err)
		}
		if _, present := fields["permission"]; present {
			t.Fatalf("opencode agent %q has agent-local permission", role)
		}
	}
	var rawMCP map[string]json.RawMessage
	if err := json.Unmarshal(rawConfig["mcp"], &rawMCP); err != nil {
		t.Fatalf("raw opencode mcp = %q, not valid JSON: %v", rawConfig["mcp"], err)
	}
	for _, name := range []string{"github", "context7"} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(rawMCP[name], &fields); err != nil {
			t.Fatalf("raw %s mcp = %q, not valid JSON: %v", name, rawMCP[name], err)
		}
		if got := string(fields["oauth"]); got != "false" {
			t.Fatalf("raw %s mcp oauth = %q, want false", name, got)
		}
	}
	var metricsFields map[string]json.RawMessage
	if err := json.Unmarshal(rawMCP["metrics"], &metricsFields); err != nil {
		t.Fatalf("raw metrics mcp = %q, not valid JSON: %v", rawMCP["metrics"], err)
	}
	if _, present := metricsFields["oauth"]; present {
		t.Fatal("raw metrics mcp contains oauth")
	}
	for _, mount := range container.VolumeMounts {
		if mount.Name != "opencode-config" {
			continue
		}
		if !mount.ReadOnly {
			t.Fatal("opencode config volume mount is not read-only")
		}
		if mount.MountPath != opencodeConfigMountPath {
			t.Fatalf("opencode config mount path = %q, want %q", mount.MountPath, opencodeConfigMountPath)
		}
		if mount.MountPath == "/workspace" {
			t.Fatal("opencode config is mounted inside the workspace")
		}
		return
	}
	t.Fatal("opencode-config volume mount is missing")
}

func TestBuildCoordinatorPodOmitsMetricsMCPWhenUnconfigured(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-no-metrics", Namespace: "courier-system"},
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
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "litellm/coordinator"}},
	}
	pod, err := BuildCoordinatorPod(run, lane, PodConfig{
		Image:          "registry.example/courier-opencode:test",
		WorkspacePath:  "/workspace",
		GitHubMCPURL:   "https://github-mcp.example/mcp",
		Context7MCPURL: "https://context7.example/mcp",
	})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	annotation, ok := pod.Annotations[opencodeConfigAnnotation]
	if !ok {
		t.Fatal("opencode config annotation is missing")
	}
	var cfg openCodeConfig
	if err := json.Unmarshal([]byte(annotation), &cfg); err != nil {
		t.Fatalf("opencode config annotation = %q, not valid JSON: %v", annotation, err)
	}
	if len(cfg.MCP) != 2 {
		t.Fatalf("opencode config mcp = %#v, want exactly github and context7", cfg.MCP)
	}
	if _, present := cfg.MCP["metrics"]; present {
		t.Fatalf("opencode config has a metrics mcp entry: %#v", cfg.MCP["metrics"])
	}
	if got, ok := cfg.MCP["github"]; !ok || got.URL != "https://github-mcp.example/mcp" {
		t.Fatalf("github mcp = %#v, want the configured github URL", got)
	}
	if got, ok := cfg.MCP["context7"]; !ok || got.URL != "https://context7.example/mcp" {
		t.Fatalf("context7 mcp = %#v, want the configured context7 URL", got)
	}
}

func TestBuildCoordinatorPodProvisionsScratch(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-scratch",
			Namespace: "courier-system",
			UID:       "run-uid",
		},
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
		Spec:       courierv1alpha1.LaneProfileSpec{Roles: map[string]string{"coordinator": "litellm/qwen"}},
	}

	const scratchMountPath = "/var/tmp/courier-scratch"

	pod, err := BuildCoordinatorPod(run, lane, DefaultPodConfig())
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}

	foundScratchVolume := false
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != "scratch" {
			continue
		}
		if volume.EmptyDir == nil {
			t.Fatal("scratch volume is not an EmptyDir; give the scratch volume an EmptyDir source in BuildCoordinatorPod")
		}
		foundScratchVolume = true
		break
	}
	if !foundScratchVolume {
		t.Fatal("scratch EmptyDir volume is missing; add a volume named \"scratch\" in BuildCoordinatorPod")
	}

	container := pod.Spec.Containers[0]

	foundScratchMount := false
	for _, mount := range container.VolumeMounts {
		if mount.Name != "scratch" {
			continue
		}
		if mount.MountPath != scratchMountPath {
			t.Fatalf("scratch mount path = %q, want %q", mount.MountPath, scratchMountPath)
		}
		if mount.ReadOnly {
			t.Fatal("scratch volume mount is read-only; the coordinator must write to it")
		}
		foundScratchMount = true
		break
	}
	if !foundScratchMount {
		t.Fatal("scratch volume mount is missing; mount the scratch volume at /var/tmp/courier-scratch")
	}

	env := make(map[string]string, len(container.Env))
	for _, value := range container.Env {
		env[value.Name] = value.Value
	}
	for _, key := range []string{"TMPDIR", "TMP", "TEMP", "COURIER_SCRATCH_DIR"} {
		if env[key] != scratchMountPath {
			t.Fatalf("env %s = %q, want %q", key, env[key], scratchMountPath)
		}
	}

	if scratchMountPath == container.WorkingDir {
		t.Fatalf("scratch mount path %q collides with the workspace mount path", scratchMountPath)
	}
	if scratchMountPath == "/courier-runtime" {
		t.Fatalf("scratch mount path %q collides with the runtime mount path", scratchMountPath)
	}
}

func TestOpenCodeConfigScratchPermissionLeastPrivilege(t *testing.T) {
	roles := map[string]string{
		"coordinator": "litellm/qwen",
		"coder":       "litellm/coder",
	}
	raw, err := marshalOpenCodeConfig(roles, "", "", "")
	if err != nil {
		t.Fatalf("marshalOpenCodeConfig() error = %v", err)
	}

	var config map[string]any
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatalf("opencode config = %q, not valid JSON: %v", raw, err)
	}

	permission, ok := config["permission"].(map[string]any)
	if !ok {
		t.Fatalf("permission = %#v, want an object; emit a permission object from marshalOpenCodeConfig", config["permission"])
	}

	const scratchAllow = "/var/tmp/courier-scratch/**"

	for _, key := range []string{"github_merge_pull_request", "github_merge*"} {
		if permission[key] != "deny" {
			t.Fatalf("permission %q = %v, want \"deny\" (retained merge denial)", key, permission[key])
		}
	}

	externalDirectory, ok := permission["external_directory"].(map[string]any)
	if !ok {
		t.Fatalf("permission.external_directory = %#v, want an object mapping the scratch path to a permission", permission["external_directory"])
	}
	if len(externalDirectory) != 1 {
		t.Fatalf("permission.external_directory = %#v, want exactly one key; narrow it to the per-run scratch path", externalDirectory)
	}
	for key, value := range externalDirectory {
		if key != scratchAllow {
			t.Fatalf("permission.external_directory key = %q, want %q", key, scratchAllow)
		}
		if value != "allow" {
			t.Fatalf("permission.external_directory[%q] = %v, want \"allow\"", key, value)
		}
		if key == "*" || key == "/*" {
			t.Fatalf("permission.external_directory uses the wildcard key %q; scope it to the scratch path", key)
		}
		if strings.HasPrefix(key, "/tmp") {
			t.Fatalf("permission.external_directory allows a path beginning /tmp: %q; scope it to the scratch path", key)
		}
		if strings.Contains(key, "~") || strings.Contains(key, "$HOME") || strings.Contains(key, "root") {
			t.Fatalf("permission.external_directory references a home/config path: %q", key)
		}
	}
}

func TestCoordinatorPromptIncludesScratchHint(t *testing.T) {
	const scratchPath = "/var/tmp/courier-scratch"
	const framing = "single GPU; keep parallelism modest"
	tests := []struct {
		name    string
		goal    string
		framing string
	}{
		{name: "with lane framing", goal: "Open a PR for issue #7.", framing: framing},
		{name: "without lane framing", goal: "Open a PR for issue #7.", framing: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invocation := Invocation{Goal: test.goal, Framing: test.framing}
			got := prompt(invocation)
			if !strings.Contains(got, scratchPath) {
				t.Fatalf("prompt = %q, missing the scratch dir hint %q", got, scratchPath)
			}
			if test.framing != "" && !strings.Contains(got, test.framing) {
				t.Fatalf("prompt = %q, missing the lane framing %q", got, test.framing)
			}
		})
	}
}

func TestPodConfigValidateScratchOverlap(t *testing.T) {
	base := DefaultPodConfig()
	for _, test := range []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "workspace", path: "/workspace", wantErr: false},
		{name: "sibling", path: "/var/tmp/other", wantErr: false},
		{name: "ancestor", path: "/var/tmp", wantErr: true},
		{name: "higher ancestor", path: "/var", wantErr: true},
		{name: "equal", path: "/var/tmp/courier-scratch", wantErr: true},
		{name: "descendant", path: "/var/tmp/courier-scratch/sub", wantErr: true},
		{name: "trailing-slash equal", path: "/var/tmp/courier-scratch/", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base.WorkspacePath = test.path
			err := base.Validate()
			if test.wantErr && err == nil {
				t.Fatalf("Validate() for workspace path %q = nil, want an overlap error", test.path)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("Validate() for workspace path %q = %v, want no error", test.path, err)
			}
		})
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
