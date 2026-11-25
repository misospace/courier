package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

// plainExecutor is a non-observer runtime so a test can drive the App-mode
// launch path without the isolated observer also claiming the pod identity.
type plainExecutor struct{}

func (plainExecutor) Name() string { return "plain" }

func (plainExecutor) Command(executor.Invocation) executor.Command {
	return executor.Command{Binary: "/bin/true"}
}

func (plainExecutor) Result(int, error) executor.Outcome { return executor.Outcome{} }

func installationLegacyRun(name string) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "local", courierv1alpha1.PhaseClaimed)
	run.Spec.Repo = "Acme/Widgets"
	run.UID = types.UID("uid-" + name)
	return run
}

func installationLauncher(c client.Client, minter InstallationTokenMinter, now time.Time, exec executor.Executor) *CoordinatorLauncher {
	return &CoordinatorLauncher{
		Client:    c,
		APIReader: c,
		Pod:       executor.PodConfig{Image: "example/coordinator:test", GitRemoteURL: "https://github.com/%s.git"},
		Executor:  exec,
		InstallationGit: &InstallationGitOptions{
			Minter: minter,
			Now:    func() time.Time { return now },
		},
	}
}

func legacyDocumentRaw(t *testing.T, document topology.InstallationTokenDocument) []byte {
	t.Helper()
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func expectNeedsHumanReason(t *testing.T, err error, reason string) {
	t.Helper()
	permanent := (*SecureNeedsHumanError)(nil)
	if !errors.As(err, &permanent) || permanent.Reason != reason {
		t.Fatalf("err = %v, want needs-human %s", err, reason)
	}
}

func getOrFail(t *testing.T, c client.Client, key client.ObjectKey, obj client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), key, obj); err != nil {
		t.Fatalf("get %s: %v", key.Name, err)
	}
}

func expectAbsent(t *testing.T, c client.Client, key client.ObjectKey, obj client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), key, obj); !apierrors.IsNotFound(err) {
		t.Fatalf("get %s err = %v, want NotFound", key.Name, err)
	}
}

func TestLaunchWithoutInstallationGitWritesNoCredentials(t *testing.T) {
	// A static deployment is byte-identical: no credentials Secret, no
	// run-scoped git-credentials identity, and no installation contract on
	// the pod — only today's observer behavior.
	run := installationLegacyRun("static-run")
	c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
	launcher := &CoordinatorLauncher{
		Client:    c,
		APIReader: c,
		Pod:       executor.PodConfig{Image: "example/opencode:test", GitRemoteURL: "https://github.com/%s.git"},
		Executor:  &executor.OpenCode{},
	}
	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	nsKey := func(name string) client.ObjectKey {
		return client.ObjectKey{Namespace: admissionNamespace, Name: name}
	}
	expectAbsent(t, c, nsKey(topology.CredentialsSecretName(run.Name)), &corev1.Secret{})
	expectAbsent(t, c, nsKey(legacyGitCredentialsResourceName(run.Name)), &corev1.ServiceAccount{})
	expectAbsent(t, c, nsKey(legacyGitCredentialsResourceName(run.Name)), &rbacv1.Role{})
	expectAbsent(t, c, nsKey(legacyGitCredentialsResourceName(run.Name)), &rbacv1.RoleBinding{})
	var pod corev1.Pod
	getOrFail(t, c, nsKey("static-run-coordinator"), &pod)
	for _, container := range pod.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == executor.EnvGitInstallation || env.Name == executor.EnvInstallationOwner || env.Name == executor.EnvInstallationEndpoint {
				t.Fatalf("static pod carries installation env %q", env.Name)
			}
		}
	}
	// Today's observer identity is untouched behavior, still provisioned.
	expectObserved := legacyObserverResourceName(run.Name)
	getOrFail(t, c, nsKey(expectObserved), &corev1.ServiceAccount{})
}

func TestLaunchGitGuardPolicyLifecycle(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	cidrs := []string{"140.82.112.0/20"}

	t.Run("policy precedes App-mode pod", func(t *testing.T) {
		run := installationLegacyRun("guard-order")
		created := []client.Object{}
		base := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			created = append(created, obj.DeepCopyObject().(client.Object))
			return c.Create(ctx, obj, opts...)
		}})
		launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "guard", expiresAt: now.Add(2 * time.Hour)}, now, plainExecutor{})
		launcher.GitGuardEgressCIDRs = cidrs
		if err := launcher.Launch(context.Background(), run); err != nil {
			t.Fatalf("Launch: %v", err)
		}
		policyPosition, podPosition := -1, -1
		for i, obj := range created {
			switch obj.(type) {
			case *networkingv1.NetworkPolicy:
				policyPosition = i
			case *corev1.Pod:
				podPosition = i
			}
		}
		if policyPosition < 0 || podPosition < 0 || policyPosition >= podPosition {
			t.Fatalf("create order = %T sequence %v, policy position %d pod position %d", created, created, policyPosition, podPosition)
		}
	})

	t.Run("flag requires App-mode before pod create", func(t *testing.T) {
		run := installationLegacyRun("guard-static")
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		launcher := &CoordinatorLauncher{Client: c, APIReader: c, Pod: executor.PodConfig{Image: "example/coordinator:test"}, Executor: plainExecutor{}, GitGuardEgressCIDRs: cidrs}
		err := launcher.Launch(context.Background(), run)
		expectNeedsHumanReason(t, err, "GitGuardPolicyApp")
		expectAbsent(t, c, client.ObjectKey{Namespace: run.Namespace, Name: run.Name + "-coordinator"}, &corev1.Pod{})
	})

	t.Run("unset flag creates no policy", func(t *testing.T) {
		run := installationLegacyRun("guard-disabled")
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "guard", expiresAt: now.Add(2 * time.Hour)}, now, plainExecutor{})
		if err := launcher.Launch(context.Background(), run); err != nil {
			t.Fatalf("Launch: %v", err)
		}
		expectAbsent(t, c, client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}, &networkingv1.NetworkPolicy{})
	})

	t.Run("foreign same-name policy fails closed", func(t *testing.T) {
		run := installationLegacyRun("guard-foreign")
		foreign := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}}
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run, foreign)
		launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "guard", expiresAt: now.Add(2 * time.Hour)}, now, plainExecutor{})
		launcher.GitGuardEgressCIDRs = cidrs
		err := launcher.Launch(context.Background(), run)
		expectNeedsHumanReason(t, err, "GitGuardPolicyOwnership")
		expectAbsent(t, c, client.ObjectKey{Namespace: run.Namespace, Name: run.Name + "-coordinator"}, &corev1.Pod{})
	})

	t.Run("owned matching policy is adopted idempotently", func(t *testing.T) {
		run := installationLegacyRun("guard-adopt")
		seeded := topology.GitGuardEgressPolicy(run, mustParseCIDRs(t, cidrs))
		seeded.OwnerReferences = runOwnerReferences(run)
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run, seeded)
		launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "guard", expiresAt: now.Add(2 * time.Hour)}, now, plainExecutor{})
		launcher.GitGuardEgressCIDRs = cidrs
		if err := launcher.Launch(context.Background(), run); err != nil {
			t.Fatalf("Launch: %v", err)
		}
		getOrFail(t, c, client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}, &networkingv1.NetworkPolicy{})
	})

	t.Run("owned drifted policy converges before the pod", func(t *testing.T) {
		run := installationLegacyRun("guard-drift")
		seeded := topology.GitGuardEgressPolicy(run, mustParseCIDRs(t, cidrs))
		seeded.OwnerReferences = runOwnerReferences(run)
		seeded.Spec.Egress = nil
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run, seeded)
		launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "guard", expiresAt: now.Add(2 * time.Hour)}, now, plainExecutor{})
		launcher.GitGuardEgressCIDRs = cidrs
		if err := launcher.Launch(context.Background(), run); err != nil {
			t.Fatalf("Launch: %v", err)
		}
		var live networkingv1.NetworkPolicy
		getOrFail(t, c, client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}, &live)
		want := topology.GitGuardEgressPolicy(run, mustParseCIDRs(t, cidrs))
		if !apiequality.Semantic.DeepEqual(want.Spec, live.Spec) {
			t.Fatalf("drifted policy did not converge to the rendered spec: %+v", live.Spec)
		}
	})

	t.Run("created policy admits exactly the configured CIDRs", func(t *testing.T) {
		run := installationLegacyRun("guard-content")
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "guard", expiresAt: now.Add(2 * time.Hour)}, now, plainExecutor{})
		launcher.GitGuardEgressCIDRs = cidrs
		if err := launcher.Launch(context.Background(), run); err != nil {
			t.Fatalf("Launch: %v", err)
		}
		var live networkingv1.NetworkPolicy
		getOrFail(t, c, client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}, &live)
		want := topology.GitGuardEgressPolicy(run, mustParseCIDRs(t, cidrs))
		if !apiequality.Semantic.DeepEqual(want.Spec, live.Spec) {
			t.Fatalf("created policy spec = %+v, want the rendered spec %+v", live.Spec, want.Spec)
		}
		if got := live.Labels["courier.misospace.dev/component"]; got != topology.ComponentCoordinator {
			t.Fatalf("created policy component label = %q", got)
		}
	})
}

// runOwnerReferences is the controller OwnerReference the launcher's own
// resources carry, so a test can seed a run-owned object.
func runOwnerReferences(run *courierv1alpha1.CoderRun) []metav1.OwnerReference {
	return []metav1.OwnerReference{{
		APIVersion:         courierv1alpha1.GroupVersion.String(),
		Kind:               "CoderRun",
		Name:               run.Name,
		UID:                run.UID,
		Controller:         boolPtr(true),
		BlockOwnerDeletion: boolPtr(true),
	}}
}

func mustParseCIDRs(t *testing.T, raw []string) []*net.IPNet {
	t.Helper()
	parsed, err := topology.ParseGitGuardCIDRs(raw)
	if err != nil {
		t.Fatalf("ParseGitGuardCIDRs: %v", err)
	}
	return parsed
}

func TestLaunchInstallationGitSameRepo(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	run := installationLegacyRun("app-same")
	c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
	minter := &fixedInstallationTokenMinter{token: "app-token", expiresAt: now.Add(2 * time.Hour)}
	launcher := installationLauncher(c, minter, now, plainExecutor{})

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch: %v", err)
	}

	// A same-repo run is one mint with the merged git-transport profile.
	if len(minter.mints) != 1 {
		t.Fatalf("mints = %+v, want exactly one", minter.mints)
	}
	if !reflect.DeepEqual(minter.mints[0].repositories, []string{"Acme/Widgets"}) {
		t.Fatalf("mint repositories = %v", minter.mints[0].repositories)
	}
	wantMerged := map[string]string{"contents": "write", "metadata": "read"}
	if !reflect.DeepEqual(minter.mints[0].permissions, wantMerged) {
		t.Fatalf("same-repo merged permissions = %+v, want %+v", minter.mints[0].permissions, wantMerged)
	}

	secret := readInstallationSecret(t, c, run)
	if !metav1.IsControlledBy(secret, run) {
		t.Fatalf("credentials Secret not controlled by the run: %+v", secret.OwnerReferences)
	}
	document := decodeInstallationDocument(t, secret.Data[topology.CredKeyInstallationToken])
	if document.Version != topology.InstallationTokenDocumentVersion ||
		document.Owner != "Acme" || document.Endpoint != "https://api.github.com/" || len(document.Entries) != 1 {
		t.Fatalf("document = %+v", document)
	}

	// The run-scoped identity grants exactly one get on the credentials Secret.
	name := legacyGitCredentialsResourceName(run.Name)
	var role rbacv1.Role
	getOrFail(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: name}, &role)
	if len(role.Rules) != 1 {
		t.Fatalf("role rules = %+v, want exactly one rule", role.Rules)
	}
	rule := role.Rules[0]
	if !reflect.DeepEqual(rule.APIGroups, []string{""}) ||
		!reflect.DeepEqual(rule.Resources, []string{"secrets"}) ||
		!reflect.DeepEqual(rule.ResourceNames, []string{topology.CredentialsSecretName(run.Name)}) ||
		!reflect.DeepEqual(rule.Verbs, []string{"get"}) {
		t.Fatalf("role rule grants more than get on the credentials Secret: %+v", rule)
	}
	var binding rbacv1.RoleBinding
	getOrFail(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: name}, &binding)
	if binding.RoleRef.Name != name || len(binding.Subjects) != 1 ||
		binding.Subjects[0].Name != name || binding.Subjects[0].Kind != rbacv1.ServiceAccountKind {
		t.Fatalf("role binding does not bind the run-scoped git-credentials identity: %+v", binding)
	}

	var pod corev1.Pod
	getOrFail(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: "app-same-coordinator"}, &pod)
	if pod.Spec.ServiceAccountName != name {
		t.Fatalf("pod service account = %q, want the run-scoped git-credentials identity %q", pod.Spec.ServiceAccountName, name)
	}
}

func TestLaunchInstallationGitForkRun(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	run := installationLegacyRun("app-fork")
	run.Status.HeadRepo = "Acme/Forks"
	c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
	minter := &fixedInstallationTokenMinter{token: "fork-token", expiresAt: now.Add(2 * time.Hour)}
	launcher := installationLauncher(c, minter, now, plainExecutor{})

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	// Fork: the base is read-only transport, the head is push transport, and
	// neither mint widens the other.
	if len(minter.mints) != 2 {
		t.Fatalf("mints = %+v, want two", minter.mints)
	}
	if !reflect.DeepEqual(minter.mints[0].repositories, []string{"Acme/Widgets"}) ||
		!reflect.DeepEqual(minter.mints[0].permissions, map[string]string{"contents": "read", "metadata": "read"}) {
		t.Fatalf("base mint = %+v", minter.mints[0])
	}
	if !reflect.DeepEqual(minter.mints[1].repositories, []string{"Acme/Forks"}) ||
		!reflect.DeepEqual(minter.mints[1].permissions, map[string]string{"contents": "write", "metadata": "read"}) {
		t.Fatalf("work mint = %+v", minter.mints[1])
	}
	secret := readInstallationSecret(t, c, run)
	document := decodeInstallationDocument(t, secret.Data[topology.CredKeyInstallationToken])
	baseEntry, baseOK := document.Entry("Acme/Widgets")
	workEntry, workOK := document.Entry("Acme/Forks")
	if !baseOK || !workOK || baseEntry.Token == workEntry.Token {
		t.Fatalf("document entries = %+v", document.Entries)
	}
}

func TestLaunchInstallationGitRefusals(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

	t.Run("foreign existing secret", func(t *testing.T) {
		run := installationLegacyRun("app-foreign")
		foreign := installationSecret(run, []byte("someone-elses-credential"))
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run, foreign)
		minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		err := launcher.Launch(context.Background(), run)
		expectNeedsHumanReason(t, err, "InstallationTokenOwnership")
		if minter.calls != 0 {
			t.Fatalf("foreign Secret minted %d tokens", minter.calls)
		}
		if secret := readInstallationSecret(t, c, run); string(secret.Data[topology.CredKeyInstallationToken]) != "someone-elses-credential" {
			t.Fatal("foreign Secret data was modified")
		}
		expectAbsent(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: "app-foreign-coordinator"}, &corev1.Pod{})
	})

	t.Run("different-owner head repo", func(t *testing.T) {
		run := installationLegacyRun("app-other-owner")
		run.Status.HeadRepo = "Other/Forks"
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		err := launcher.Launch(context.Background(), run)
		expectNeedsHumanReason(t, err, "InstallationTokenOwner")
		if minter.calls != 0 {
			t.Fatalf("owner mismatch minted %d tokens", minter.calls)
		}
		expectAbsent(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: topology.CredentialsSecretName(run.Name)}, &corev1.Secret{})
	})

	t.Run("minter identity signs for another owner", func(t *testing.T) {
		run := installationLegacyRun("app-minter-owner")
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour), owner: "Other"}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		err := launcher.Launch(context.Background(), run)
		expectNeedsHumanReason(t, err, "InstallationTokenOwner")
		if minter.calls != 0 {
			t.Fatalf("identity mismatch minted %d tokens", minter.calls)
		}
	})

	t.Run("minter endpoint is not https", func(t *testing.T) {
		run := installationLegacyRun("app-http-endpoint")
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour), owner: "Acme", endpoint: "http://api.github.com/"}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		err := launcher.Launch(context.Background(), run)
		expectNeedsHumanReason(t, err, "InstallationTokenEndpoint")
		if minter.calls != 0 {
			t.Fatalf("endpoint mismatch minted %d tokens", minter.calls)
		}
	})

	t.Run("short mint is transient and writes nothing", func(t *testing.T) {
		run := installationLegacyRun("app-short-mint")
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
		minter := &fixedInstallationTokenMinter{token: "short", expiresAt: now.Add(10 * time.Minute)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		err := launcher.Launch(context.Background(), run)
		if err == nil {
			t.Fatal("short mint err = nil, want a retryable refusal")
		}
		expectNeedsHuman := (*SecureNeedsHumanError)(nil)
		if errors.As(err, &expectNeedsHuman) {
			t.Fatalf("short mint err = %v, must stay retryable, not needs-human", err)
		}
		if minter.calls != 1 {
			t.Fatalf("short mint calls = %d, want 1", minter.calls)
		}
		expectAbsent(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: topology.CredentialsSecretName(run.Name)}, &corev1.Secret{})
		expectAbsent(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: "app-short-mint-coordinator"}, &corev1.Pod{})
	})

	t.Run("fresh document mints nothing", func(t *testing.T) {
		run := installationLegacyRun("app-fresh")
		document := topology.InstallationTokenDocument{
			Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/",
			Entries: []topology.InstallationTokenEntry{
				{Repository: "Acme/Widgets", Token: "fresh-token", ExpiresAt: now.Add(2 * time.Hour)},
			},
		}
		owned := installationSecret(run, legacyDocumentRaw(t, document), controllerOwnerRef(run.Name, run.UID))
		c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run, owned)
		minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		if err := launcher.Launch(context.Background(), run); err != nil {
			t.Fatalf("Launch with a fresh document: %v", err)
		}
		if minter.calls != 0 {
			t.Fatalf("fresh document caused %d mints, want 0", minter.calls)
		}
		getOrFail(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: "app-fresh-coordinator"}, &corev1.Pod{})
	})

	t.Run("create race with an uncontrolled winner fails closed", func(t *testing.T) {
		run := installationLegacyRun("app-race")
		secretName := topology.CredentialsSecretName(run.Name)
		winner := installationSecret(run, []byte("winner-foreign"))
		firstGet := true
		c := fake.NewClientBuilder().
			WithScheme(schemeForAdmission(t)).
			WithStatusSubresource(&courierv1alpha1.CoderRun{}).
			WithRuntimeObjects(admissionLaneWithCoordinator("local", 1), run, winner).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if key.Name == secretName && key.Namespace == run.Namespace && firstGet {
						firstGet = false
						return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
					}
					return c.Get(ctx, key, obj, opts...)
				},
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if obj.GetName() == secretName && obj.GetNamespace() == run.Namespace {
						return apierrors.NewAlreadyExists(corev1.Resource("secrets"), obj.GetName())
					}
					if p, ok := obj.(*corev1.Pod); ok {
						t.Fatalf("coordinator pod created after the create-race refusal: %s", p.Name)
					}
					return c.Create(ctx, obj, opts...)
				},
			}).Build()
		minter := &fixedInstallationTokenMinter{token: "loser", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		err := launcher.Launch(context.Background(), run)
		expectNeedsHumanReason(t, err, "InstallationTokenOwnership")
	})
}

func TestLaunchInstallationGitWithObserver(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	run := installationLegacyRun("app-observer")
	c := launcherClientForAdmission(t, admissionLaneWithCoordinator("local", 1), run)
	minter := &fixedInstallationTokenMinter{token: "obs-token", expiresAt: now.Add(2 * time.Hour)}
	launcher := installationLauncher(c, minter, now, &executor.OpenCode{})

	if err := launcher.Launch(context.Background(), run); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	observerName := legacyObserverResourceName(run.Name)
	credsName := legacyGitCredentialsResourceName(run.Name)
	var binding rbacv1.RoleBinding
	getOrFail(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: credsName}, &binding)
	if len(binding.Subjects) != 1 || binding.Subjects[0].Name != observerName {
		t.Fatalf("credential RoleBinding subject = %+v, want the observer service account %q", binding.Subjects, observerName)
	}
	var pod corev1.Pod
	getOrFail(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: "app-observer-coordinator"}, &pod)
	if pod.Spec.ServiceAccountName != observerName {
		t.Fatalf("pod service account = %q, want the observer identity unchanged", pod.Spec.ServiceAccountName)
	}
	// The observer is already the pod identity: no second ServiceAccount.
	expectAbsent(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: credsName}, &corev1.ServiceAccount{})
	getOrFail(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: topology.CredentialsSecretName(run.Name)}, &corev1.Secret{})
}

func TestEnsureInstallationTokenRotation(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

	t.Run("near-expiry document is re-minted and updated", func(t *testing.T) {
		run := installationLegacyRun("rotate-soon")
		document := topology.InstallationTokenDocument{
			Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/",
			Entries: []topology.InstallationTokenEntry{
				{Repository: "Acme/Widgets", Token: "stale-token", ExpiresAt: now.Add(10 * time.Minute)},
			},
		}
		owned := installationSecret(run, legacyDocumentRaw(t, document), controllerOwnerRef(run.Name, run.UID))
		c := launcherClientForAdmission(t, run, owned)
		minter := &fixedInstallationTokenMinter{token: "rotated", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		if err := launcher.EnsureInstallationToken(context.Background(), run); err != nil {
			t.Fatalf("EnsureInstallationToken: %v", err)
		}
		if minter.calls != 1 {
			t.Fatalf("near-expiry document minted %d tokens, want 1", minter.calls)
		}
		secret := readInstallationSecret(t, c, run)
		rotated := decodeInstallationDocument(t, secret.Data[topology.CredKeyInstallationToken])
		if entry, ok := rotated.Entry("Acme/Widgets"); !ok || entry.Token != "rotated-1" {
			t.Fatalf("rotated document = %+v", rotated)
		}
		var superseded []topology.InstallationTokenEntry
		if err := json.Unmarshal(secret.Data[topology.CredKeyInstallationTokenSuperseded], &superseded); err != nil {
			t.Fatalf("superseded entries = %s: %v", secret.Data[topology.CredKeyInstallationTokenSuperseded], err)
		}
		if len(superseded) != 1 || superseded[0].Token != "stale-token" {
			t.Fatalf("superseded = %+v, want the one replaced live credential", superseded)
		}
	})

	t.Run("superseded list drops expired and replaced tokens", func(t *testing.T) {
		run := installationLegacyRun("rotate-superseded")
		document := topology.InstallationTokenDocument{
			Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/",
			Entries: []topology.InstallationTokenEntry{
				{Repository: "Acme/Widgets", Token: "live-token", ExpiresAt: now.Add(10 * time.Minute)},
			},
		}
		previous, err := json.Marshal([]topology.InstallationTokenEntry{
			{Repository: "Acme/Widgets", Token: "expired-token", ExpiresAt: now.Add(-time.Minute)},
			{Repository: "Acme/Widgets", Token: "live-token", ExpiresAt: now.Add(10 * time.Minute)},
			{Repository: "Acme/Widgets", Token: "rotated-1", ExpiresAt: now.Add(2 * time.Hour)},
			{Repository: "Acme/Widgets", Token: "older-but-valid", ExpiresAt: now.Add(30 * time.Minute)},
		})
		if err != nil {
			t.Fatal(err)
		}
		owned := installationSecret(run, legacyDocumentRaw(t, document), controllerOwnerRef(run.Name, run.UID))
		owned.Data[topology.CredKeyInstallationTokenSuperseded] = previous
		c := launcherClientForAdmission(t, run, owned)
		minter := &fixedInstallationTokenMinter{token: "rotated", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		if err := launcher.EnsureInstallationToken(context.Background(), run); err != nil {
			t.Fatalf("EnsureInstallationToken: %v", err)
		}
		var superseded []topology.InstallationTokenEntry
		raw := readInstallationSecret(t, c, run).Data[topology.CredKeyInstallationTokenSuperseded]
		if err := json.Unmarshal(raw, &superseded); err != nil {
			t.Fatalf("superseded entries = %s: %v", raw, err)
		}
		if len(superseded) != 2 {
			t.Fatalf("superseded = %+v, want the live credential plus the one still-valid carry-over", superseded)
		}
		tokens := map[string]bool{}
		for _, entry := range superseded {
			tokens[entry.Token] = true
		}
		if !tokens["live-token"] || !tokens["older-but-valid"] || tokens["expired-token"] || tokens["rotated-1"] {
			t.Fatalf("superseded = %+v, want live-token and older-but-valid only", superseded)
		}
	})

	// An all-expired rotation must leave no superseded key whatever the
	// stored history looked like: expired entries are dropped, and a stored
	// list that no longer parses carries no recoverable credential.
	expiredHistory, err := json.Marshal([]topology.InstallationTokenEntry{
		{Repository: "Acme/Widgets", Token: "dead-superseded", ExpiresAt: now.Add(-time.Hour)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, history := range []struct {
		name string
		raw  []byte
	}{
		{name: "no stored history", raw: nil},
		{name: "expired history", raw: expiredHistory},
		{name: "unreadable history", raw: []byte("{not json")},
	} {
		t.Run("all-expired rotation with "+history.name+" leaves no superseded key", func(t *testing.T) {
			run := installationLegacyRun("rotate-all-expired")
			document := topology.InstallationTokenDocument{
				Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/",
				Entries: []topology.InstallationTokenEntry{
					{Repository: "Acme/Widgets", Token: "dead-token", ExpiresAt: now.Add(-time.Hour)},
				},
			}
			owned := installationSecret(run, legacyDocumentRaw(t, document), controllerOwnerRef(run.Name, run.UID))
			if history.raw != nil {
				owned.Data[topology.CredKeyInstallationTokenSuperseded] = history.raw
			}
			c := launcherClientForAdmission(t, run, owned)
			minter := &fixedInstallationTokenMinter{token: "fresh", expiresAt: now.Add(2 * time.Hour)}
			launcher := installationLauncher(c, minter, now, plainExecutor{})
			if err := launcher.EnsureInstallationToken(context.Background(), run); err != nil {
				t.Fatalf("EnsureInstallationToken: %v", err)
			}
			if secret := readInstallationSecret(t, c, run); len(secret.Data) != 1 {
				keys := make([]string, 0, len(secret.Data))
				for key := range secret.Data {
					keys = append(keys, key)
				}
				t.Fatalf("expired-only rotation left keys %v, want only the document key", keys)
			}
		})
	}

	t.Run("missing document is recreated only for an App-mode coordinator", func(t *testing.T) {
		run := installationLegacyRun("rotate-recreate")
		pod := legacyCoordinatorPod(run, true)
		c := launcherClientForAdmission(t, run, pod)
		minter := &fixedInstallationTokenMinter{token: "recreated", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		if err := launcher.EnsureInstallationToken(context.Background(), run); err != nil {
			t.Fatalf("EnsureInstallationToken: %v", err)
		}
		if minter.calls != 1 {
			t.Fatalf("App-mode recreation minted %d tokens, want 1", minter.calls)
		}
		secret := readInstallationSecret(t, c, run)
		if !metav1.IsControlledBy(secret, run) {
			t.Fatal("recreated Secret not controlled by the run")
		}
	})

	t.Run("guard-shaped App pod is recreated on the annotation alone", func(t *testing.T) {
		run := installationLegacyRun("rotate-guard")
		pod := legacyGuardCoordinatorPod(run)
		c := launcherClientForAdmission(t, run, pod)
		minter := &fixedInstallationTokenMinter{token: "guarded", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		if err := launcher.EnsureInstallationToken(context.Background(), run); err != nil {
			t.Fatalf("EnsureInstallationToken: %v", err)
		}
		if minter.calls != 1 {
			t.Fatalf("guard-shaped recreation minted %d tokens, want 1", minter.calls)
		}
		if secret := readInstallationSecret(t, c, run); !metav1.IsControlledBy(secret, run) {
			t.Fatal("guard-shaped recreation did not produce a run-controlled Secret")
		}
	})

	t.Run("static run never gains a Secret behind its back", func(t *testing.T) {
		run := installationLegacyRun("rotate-static")
		pod := legacyCoordinatorPod(run, false)
		c := launcherClientForAdmission(t, run, pod)
		minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		if err := launcher.EnsureInstallationToken(context.Background(), run); err != nil {
			t.Fatalf("EnsureInstallationToken on a static run: %v", err)
		}
		if minter.calls != 0 {
			t.Fatalf("static run minted %d tokens, want 0", minter.calls)
		}
		expectAbsent(t, c, client.ObjectKey{Namespace: admissionNamespace, Name: topology.CredentialsSecretName(run.Name)}, &corev1.Secret{})
	})

	t.Run("foreign Secret is an error and never overwritten", func(t *testing.T) {
		run := installationLegacyRun("rotate-foreign")
		foreign := installationSecret(run, []byte("someone-elses-credential"))
		c := launcherClientForAdmission(t, run, foreign)
		minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour)}
		launcher := installationLauncher(c, minter, now, plainExecutor{})
		err := launcher.EnsureInstallationToken(context.Background(), run)
		expectNeedsHumanReason(t, err, "InstallationTokenOwnership")
		if minter.calls != 0 {
			t.Fatalf("foreign Secret minted %d tokens", minter.calls)
		}
		if secret := readInstallationSecret(t, c, run); string(secret.Data[topology.CredKeyInstallationToken]) != "someone-elses-credential" {
			t.Fatal("foreign Secret data was modified")
		}
	})
}

func TestInstallationTokenRequeueDelayLegacy(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	run := installationLegacyRun("rotate-delay")
	c := launcherClientForAdmission(t, run)
	minter := &fixedInstallationTokenMinter{token: "delay", expiresAt: now.Add(2 * time.Hour)}
	launcher := installationLauncher(c, minter, now, plainExecutor{})

	// No document yet: the bounded default, not a deadline.
	if delay := launcher.InstallationTokenRequeueDelay(context.Background(), run); delay != legacyInstallationTokenAbsentRequeue {
		t.Fatalf("delay without a document = %v, want %v", delay, legacyInstallationTokenAbsentRequeue)
	}

	write := func(entries ...topology.InstallationTokenEntry) {
		t.Helper()
		raw := legacyDocumentRaw(t, topology.InstallationTokenDocument{
			Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/",
			Entries: entries,
		})
		secret := readInstallationSecret(t, c, run)
		secret.Data = map[string][]byte{topology.CredKeyInstallationToken: raw}
		if err := c.Update(context.Background(), secret); err != nil {
			t.Fatal(err)
		}
	}
	if err := launcher.EnsureInstallationToken(context.Background(), run); err != nil {
		// No App-mode coordinator exists, so the ensure must be a no-op —
		// and the delay still reads the absent document as the default.
		t.Fatalf("EnsureInstallationToken without an App-mode coordinator: %v", err)
	}
	if delay := launcher.InstallationTokenRequeueDelay(context.Background(), run); delay != legacyInstallationTokenAbsentRequeue {
		t.Fatalf("delay with no Secret = %v, want %v", delay, legacyInstallationTokenAbsentRequeue)
	}
	owned := installationSecret(run, legacyDocumentRaw(t, topology.InstallationTokenDocument{Version: 99}), controllerOwnerRef(run.Name, run.UID))
	if err := c.Create(context.Background(), owned); err != nil {
		t.Fatal(err)
	}
	// Wrong version: no usable document.
	if delay := launcher.InstallationTokenRequeueDelay(context.Background(), run); delay != legacyInstallationTokenAbsentRequeue {
		t.Fatalf("delay for a wrong-version document = %v, want %v", delay, legacyInstallationTokenAbsentRequeue)
	}

	// A token 2h out with a 25m refresh lead is refreshed in ~95m.
	write(topology.InstallationTokenEntry{Repository: "Acme/Widgets", Token: "one", ExpiresAt: now.Add(2 * time.Hour)})
	if delay := launcher.InstallationTokenRequeueDelay(context.Background(), run); delay != 95*time.Minute {
		t.Fatalf("delay for a fresh token = %v, want 95m", delay)
	}
	// The earliest entry schedules the pass.
	write(
		topology.InstallationTokenEntry{Repository: "Acme/Widgets", Token: "later", ExpiresAt: now.Add(2 * time.Hour)},
		topology.InstallationTokenEntry{Repository: "Acme/Forks", Token: "sooner", ExpiresAt: now.Add(90 * time.Minute)},
	)
	if delay := launcher.InstallationTokenRequeueDelay(context.Background(), run); delay != 65*time.Minute {
		t.Fatalf("delay for the earliest entry = %v, want 65m", delay)
	}
	// Inside the refresh window the floor is the broker margin, never zero:
	// a zero RequeueAfter schedules no pass at all, and a near-expiry
	// document must still get its rotation pass.
	write(topology.InstallationTokenEntry{Repository: "Acme/Widgets", Token: "soon", ExpiresAt: now.Add(10 * time.Minute)})
	if delay := launcher.InstallationTokenRequeueDelay(context.Background(), run); delay != installationTokenBrokerMargin {
		t.Fatalf("delay inside the window = %v, want %v", delay, installationTokenBrokerMargin)
	}
}

func TestObserveRunningSettleLegacyGitCredentials(t *testing.T) {
	stub := &stubLegacyGitCredentials{delay: 45 * time.Minute}
	running := func(name string, appMode bool) (*courierv1alpha1.CoderRun, *corev1.Pod) {
		run := &courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: admissionNamespace, UID: types.UID("uid-" + name)},
			Spec:       courierv1alpha1.CoderRunSpec{Mode: courierv1alpha1.ModeResolveIssue, Repo: "Acme/Widgets", Ref: 1, Lane: "local"},
			Status:     courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
		}
		return run, legacyCoordinatorPod(run, appMode)
	}

	t.Run("App-mode coordinator rotates and requeues", func(t *testing.T) {
		run, pod := running("settle-app", true)
		c := launcherClientForAdmission(t, run, pod)
		r := &CoderRunReconciler{Client: c, APIReader: c, LegacyGitCredentials: stub}
		result, err := r.observeRunning(context.Background(), run)
		if err != nil {
			t.Fatalf("observeRunning: %v", err)
		}
		if result.RequeueAfter != 45*time.Minute {
			t.Fatalf("requeue = %v, want the computed 45m", result.RequeueAfter)
		}
		if stub.ensures != 1 {
			t.Fatalf("ensures = %d, want 1", stub.ensures)
		}
	})

	t.Run("static coordinator settles unchanged", func(t *testing.T) {
		run, pod := running("settle-static", false)
		c := launcherClientForAdmission(t, run, pod)
		fresh := &stubLegacyGitCredentials{delay: 45 * time.Minute}
		r := &CoderRunReconciler{Client: c, APIReader: c, LegacyGitCredentials: fresh}
		result, err := r.observeRunning(context.Background(), run)
		if err != nil {
			t.Fatalf("observeRunning: %v", err)
		}
		if result.RequeueAfter != 0 || result.Requeue {
			t.Fatalf("static settle result = %#v, want the unchanged zero result", result)
		}
		if fresh.ensures != 0 {
			t.Fatalf("static coordinator triggered %d ensures, want 0", fresh.ensures)
		}
	})

	t.Run("no seam keeps the zero result", func(t *testing.T) {
		run, pod := running("settle-nil", true)
		c := launcherClientForAdmission(t, run, pod)
		r := &CoderRunReconciler{Client: c, APIReader: c}
		result, err := r.observeRunning(context.Background(), run)
		if err != nil {
			t.Fatalf("observeRunning: %v", err)
		}
		if result.RequeueAfter != 0 || result.Requeue {
			t.Fatalf("result = %#v, want the unchanged zero result", result)
		}
	})

	t.Run("rotation errors requeue through backoff", func(t *testing.T) {
		boom := errors.New("mint unavailable")
		failing := &stubLegacyGitCredentials{err: boom}
		run, pod := running("settle-error", true)
		c := launcherClientForAdmission(t, run, pod)
		r := &CoderRunReconciler{Client: c, APIReader: c, LegacyGitCredentials: failing}
		if _, err := r.observeRunning(context.Background(), run); !errors.Is(err, boom) {
			t.Fatalf("observeRunning err = %v, want the ensure failure to propagate", err)
		}
	})
}

// stubLegacyGitCredentials records the settle's rotation calls.
type stubLegacyGitCredentials struct {
	ensures int
	delay   time.Duration
	err     error
}

func (s *stubLegacyGitCredentials) EnsureInstallationToken(_ context.Context, _ *courierv1alpha1.CoderRun) error {
	s.ensures++
	return s.err
}

func (s *stubLegacyGitCredentials) InstallationTokenRequeueDelay(_ context.Context, _ *courierv1alpha1.CoderRun) time.Duration {
	return s.delay
}

// legacyCoordinatorPod builds a run-attributed coordinator pod, optionally
// rendered with the App-mode installation contract.
func legacyCoordinatorPod(run *courierv1alpha1.CoderRun, appMode bool) *corev1.Pod {
	env := []corev1.EnvVar{{Name: "COURIER_GIT_USERNAME", Value: "courier"}}
	if appMode {
		env = append(env, corev1.EnvVar{Name: executor.EnvGitInstallation, Value: "1"})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            run.Name + "-coordinator",
			Namespace:       run.Namespace,
			Labels:          map[string]string{executor.LabelRun: executor.RunLabelValue(run.Name)},
			OwnerReferences: ownerRefsForTest(run.Name, string(run.UID)),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "coordinator", Image: "example/coordinator:test", Env: env}},
		},
	}
}

// legacyGuardCoordinatorPod builds a #902 guard-shaped App-mode pod: the
// contract is the annotation and the guard native sidecar; the coordinator
// container itself carries no credential marker.
func legacyGuardCoordinatorPod(run *courierv1alpha1.CoderRun) *corev1.Pod {
	pod := legacyCoordinatorPod(run, false)
	pod.Annotations = map[string]string{
		executor.GitEgressContractAnnotation: executor.GitEgressContractVersion,
	}
	restartAlways := corev1.ContainerRestartPolicyAlways
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
		Name: executor.GitEgressContainerName, Image: "example/coordinator:test",
		RestartPolicy: &restartAlways,
	})
	return pod
}
