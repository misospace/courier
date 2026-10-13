package executor

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

func TestGitIdentityResolvedDefaultsEmptyFields(t *testing.T) {
	resolved := GitIdentity{}.Resolved()
	if resolved != DefaultGitIdentity() {
		t.Fatalf("zero identity resolved to %#v, want the default identity", resolved)
	}
	resolved = GitIdentity{
		AuthorName:     "   ",
		AuthorEmail:    "bot@example.invalid",
		CommitterName:  "  Bot Name  ",
		CommitterEmail: "",
	}.Resolved()
	want := GitIdentity{
		AuthorName:     DefaultGitAuthorName,
		AuthorEmail:    "bot@example.invalid",
		CommitterName:  "Bot Name",
		CommitterEmail: DefaultGitAuthorEmail,
	}
	if resolved != want {
		t.Fatalf("partial identity resolved to %#v, want %#v", resolved, want)
	}
}

func TestGitIdentityEnvironmentAlwaysCarriesFourValues(t *testing.T) {
	values := map[string]string{}
	for _, env := range (GitIdentity{AuthorName: "Deploy Bot"}).Environment() {
		values[env.Name] = env.Value
	}
	if len(values) != 4 {
		t.Fatalf("identity environment has %d variables, want 4", len(values))
	}
	for name, want := range map[string]string{
		"GIT_AUTHOR_NAME":     "Deploy Bot",
		"GIT_AUTHOR_EMAIL":    DefaultGitAuthorEmail,
		"GIT_COMMITTER_NAME":  DefaultGitAuthorName,
		"GIT_COMMITTER_EMAIL": DefaultGitAuthorEmail,
	} {
		if values[name] != want {
			t.Fatalf("%s = %q, want %q", name, values[name], want)
		}
	}
}

func TestGitIdentityValidate(t *testing.T) {
	for _, test := range []struct {
		name     string
		identity GitIdentity
		wantErr  string
	}{
		{name: "all empty falls back to defaults"},
		{name: "configured", identity: GitIdentity{
			AuthorName:     "itsmiso-ai",
			AuthorEmail:    "itsmiso-ai@users.noreply.github.com",
			CommitterName:  "itsmiso-ai",
			CommitterEmail: "itsmiso-ai@users.noreply.github.com",
		}},
		{name: "newline in name", identity: GitIdentity{AuthorName: "Courier\nEvil"}, wantErr: "control characters"},
		{name: "nul in email", identity: GitIdentity{CommitterEmail: "bot\x00@example.invalid"}, wantErr: "control characters"},
		{name: "email without at sign", identity: GitIdentity{AuthorEmail: "bot.example.invalid"}, wantErr: "must contain an @"},
		{name: "email with space", identity: GitIdentity{CommitterEmail: "bot @example.invalid"}, wantErr: "whitespace"},
	} {
		err := test.identity.Validate()
		if test.wantErr == "" {
			if err != nil {
				t.Fatalf("%s: Validate() error = %v, want nil", test.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), test.wantErr) {
			t.Fatalf("%s: Validate() error = %v, want it to mention %q", test.name, err, test.wantErr)
		}
	}
}

func TestBuildCoordinatorPodRendersConfiguredGitIdentity(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-git-identity", Namespace: "courier-system"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  7,
			Lane: "go-dogfood",
		},
		Status: courierv1alpha1.CoderRunStatus{Branch: "courier/acme/widgets/issue-7"},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "go-dogfood", Namespace: "courier-system"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles: map[string]string{"coordinator": "any-model"},
		},
	}
	config := PodConfig{
		Image:             "registry.example/courier-opencode:test",
		EnvironmentSecret: "executor-env",
		Git: GitIdentity{
			AuthorName:     "itsmiso-ai",
			AuthorEmail:    "itsmiso-ai@users.noreply.github.com",
			CommitterName:  "Miso Space",
			CommitterEmail: "miso@example.invalid",
		},
	}
	pod, err := BuildCoordinatorPod(run, lane, config)
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() error = %v", err)
	}
	env := map[string]string{}
	envFromSecret := false
	for _, value := range pod.Spec.Containers[0].Env {
		env[value.Name] = value.Value
	}
	for _, source := range pod.Spec.Containers[0].EnvFrom {
		if source.SecretRef != nil && source.SecretRef.Name == "executor-env" {
			envFromSecret = true
		}
	}
	if !envFromSecret {
		t.Fatal("executor environment Secret is not wired via envFrom")
	}
	// The configured identity reaches the pod as explicit container env, so
	// it takes precedence over same-named keys in the envFrom Secret.
	for name, want := range map[string]string{
		"GIT_AUTHOR_NAME":     "itsmiso-ai",
		"GIT_AUTHOR_EMAIL":    "itsmiso-ai@users.noreply.github.com",
		"GIT_COMMITTER_NAME":  "Miso Space",
		"GIT_COMMITTER_EMAIL": "miso@example.invalid",
	} {
		if env[name] != want {
			t.Fatalf("env %s = %q, want %q", name, env[name], want)
		}
	}
	// An unconfigured identity on the same config shape keeps the explicit
	// default, preserving backward-compatible pod generation.
	defaultPod, err := BuildCoordinatorPod(run, lane, PodConfig{Image: "registry.example/courier-opencode:test"})
	if err != nil {
		t.Fatalf("BuildCoordinatorPod() default error = %v", err)
	}
	defaultEnv := map[string]string{}
	for _, value := range defaultPod.Spec.Containers[0].Env {
		defaultEnv[value.Name] = value.Value
	}
	for name, want := range map[string]string{
		"GIT_AUTHOR_NAME":     DefaultGitAuthorName,
		"GIT_AUTHOR_EMAIL":    DefaultGitAuthorEmail,
		"GIT_COMMITTER_NAME":  DefaultGitAuthorName,
		"GIT_COMMITTER_EMAIL": DefaultGitAuthorEmail,
	} {
		if defaultEnv[name] != want {
			t.Fatalf("default env %s = %q, want %q", name, defaultEnv[name], want)
		}
	}
}

func TestBuildCoordinatorPodRejectsInvalidGitIdentity(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-git-identity-invalid", Namespace: "courier-system"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode: courierv1alpha1.ModeResolveIssue,
			Repo: "acme/widgets",
			Ref:  7,
			Lane: "go-dogfood",
		},
	}
	lane := &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "go-dogfood", Namespace: "courier-system"},
		Spec: courierv1alpha1.LaneProfileSpec{
			Roles: map[string]string{"coordinator": "any-model"},
		},
	}
	_, err := BuildCoordinatorPod(run, lane, PodConfig{
		Image: "registry.example/courier-opencode:test",
		Git:   GitIdentity{AuthorEmail: "not-an-email"},
	})
	if err == nil || !strings.Contains(err.Error(), "must contain an @") {
		t.Fatalf("BuildCoordinatorPod() error = %v, want the email validation error", err)
	}
}
