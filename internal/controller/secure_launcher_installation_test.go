package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fixedInstallationTokenMinter struct {
	calls        int
	repositories []string
	permissions  map[string]string
	token        string
	expiresAt    time.Time
	err          error
	owner        string
	endpoint     string
}

func (m *fixedInstallationTokenMinter) Identity() (string, string) {
	if m.owner != "" || m.endpoint != "" {
		return m.owner, m.endpoint
	}
	return "Acme", "https://api.github.com/"
}

func (m *fixedInstallationTokenMinter) Mint(_ context.Context, repositories []string, permissions map[string]string) (string, time.Time, error) {
	m.calls++
	m.repositories = append([]string(nil), repositories...)
	m.permissions = permissions
	return m.token, m.expiresAt, m.err
}

func TestEnsureInstallationTokenCredential(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-token")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	policy := &courier.PublicationPolicy{
		RunUID: string(run.UID), ProviderConfigRef: "github", ProviderEndpoint: "https://api.github.com/",
		CredentialRefDigest: registration.Projection().Digest,
		BaseRepo:            "Acme/Widgets", WorkRepo: "Acme/Widgets",
	}
	run.Status.PublicationPolicy = policy
	minter := &fixedInstallationTokenMinter{token: "initial-token", expiresAt: now.Add(2 * time.Hour)}
	control, c := secureControl(t, run)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }

	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatalf("first ensureCredentialsCopy: %v", err)
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}
	if err := c.Get(context.Background(), key, secret); err != nil {
		t.Fatal(err)
	}
	if len(secret.Data) != 1 {
		t.Fatalf("credential Secret keys = %v, want only installation token", secret.Data)
	}
	var document topology.InstallationTokenDocument
	if err := json.Unmarshal(secret.Data[topology.CredKeyInstallationToken], &document); err != nil {
		t.Fatal(err)
	}
	if document.Token != "initial-token" || document.Owner != "Acme" || document.Endpoint != "https://api.github.com/" || document.ExpiresAt != minter.expiresAt {
		t.Fatalf("installation token document = %+v", document)
	}
	if minter.calls != 1 || len(minter.repositories) != 1 || minter.repositories[0] != policy.BaseRepo {
		t.Fatalf("mint call = %d repositories %v", minter.calls, minter.repositories)
	}
	role := topology.BrokerRoleWithInstallationToken(run)
	if len(role.Rules) != 5 || role.Rules[4].Resources[0] != "secrets" {
		t.Fatalf("installation broker Role rules = %+v", role.Rules)
	}
	registration, err = control.selectedRegistration(run)
	if err != nil {
		t.Fatal(err)
	}
	pod, err := topology.BrokerPodWithInstallationToken(run, control.Config.HarnessImage, registration.Projection(), "sa-uid", corev1.ResourceRequirements{})
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == topology.EnvForgeAPIToken || env.Name == topology.EnvGitToken {
			t.Fatalf("App-mode broker pod includes static credential env %q", env.Name)
		}
	}

	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatalf("fresh ensureCredentialsCopy: %v", err)
	}
	if minter.calls != 1 {
		t.Fatalf("fresh credential caused %d mints, want 1", minter.calls)
	}

	now = minter.expiresAt.Add(-installationTokenRefreshLead + time.Minute)
	minter.token = "rotated-token"
	minter.expiresAt = now.Add(2 * time.Hour)
	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatalf("refresh ensureCredentialsCopy: %v", err)
	}
	if minter.calls != 2 {
		t.Fatalf("refresh mint calls = %d, want 2", minter.calls)
	}
	if err := c.Get(context.Background(), key, secret); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(secret.Data[topology.CredKeyInstallationToken], &document); err != nil {
		t.Fatal(err)
	}
	if document.Token != "rotated-token" || len(secret.Data) != 1 {
		t.Fatalf("rotated credential document = %+v; keys %v", document, secret.Data)
	}
}

func TestInstallationTokenOwnerMismatchAndStaticRegression(t *testing.T) {
	run := secureRun("installation-owner-mismatch")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID: string(run.UID), ProviderConfigRef: "github", ProviderEndpoint: "https://api.github.com/",
		CredentialRefDigest: registration.Projection().Digest,
		BaseRepo:            "Acme/Widgets", WorkRepo: "Other/Widgets",
	}
	control, _ := secureControl(t, run)
	minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: time.Now().Add(time.Hour)}
	control.Config.InstallationTokenMinter = minter
	if err := control.ensureCredentialsCopy(context.Background(), run); err == nil || !strings.Contains(err.Error(), "same owner") {
		t.Fatalf("owner mismatch error = %v", err)
	}
	if minter.calls != 0 {
		t.Fatalf("owner mismatch minted %d tokens", minter.calls)
	}

	staticRun := secureRun("static-regression")
	staticRegistration, err := secureRegistry().Select(staticRun.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	staticRun.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID: string(staticRun.UID), ProviderConfigRef: "github", ProviderEndpoint: "https://api.github.com/",
		CredentialRefDigest: staticRegistration.Projection().Digest,
		BaseRepo:            "Acme/Widgets", WorkRepo: "Acme/Widgets",
	}
	staticControl, staticClient := secureControl(t, staticRun, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("static-token")},
	})
	if err := staticControl.ensureCredentialsCopy(context.Background(), staticRun); err != nil {
		t.Fatalf("static ensureCredentialsCopy: %v", err)
	}
	staticSecret := &corev1.Secret{}
	if err := staticClient.Get(context.Background(), types.NamespacedName{Namespace: staticRun.Namespace, Name: topology.CredentialsSecretName(staticRun.Name)}, staticSecret); err != nil {
		t.Fatal(err)
	}
	if len(staticSecret.Data[topology.CredKeyForgeAPIToken]) == 0 || len(staticSecret.Data[topology.CredKeyGitToken]) == 0 {
		t.Fatalf("static credentials were not copied: keys %v", staticSecret.Data)
	}

}

// TestLaunchSecureWithInstallationTokenCompletes pins the launch invariant an
// App-mode control must keep: provisioning a full topology returns a zero
// result so the caller proceeds to launch, and never a rotation requeue — a
// requeue here would leave a Claimed run resuming LaunchSecure forever
// without ever starting its control.
func TestLaunchSecureWithInstallationTokenCompletes(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-launch")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	run.Status.Phase = courier.PhaseClaimed
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID: string(run.UID), ProviderConfigRef: "github",
		ProviderEndpoint: "https://api.github.com/", CredentialRefDigest: registration.Projection().Digest,
		BaseRepo: "Acme/Widgets", BaseRef: "main", BaseOID: "baseoid",
		WorkRepo: "Acme/Widgets", WorkRef: "courier/Acme/Widgets/issue-7", WorkInitiallyAbsent: true,
	}
	minter := &fixedInstallationTokenMinter{token: "launch-token", expiresAt: now.Add(2 * time.Hour)}
	control, c := secureControl(t, run)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: secureTestNamespace,
		Labels: map[string]string{
			"pod-security.kubernetes.io/enforce": "restricted",
		},
	}}
	if err := c.Create(context.Background(), namespace); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatal(err)
	}
	result, err := control.LaunchSecure(context.Background(), run)
	if err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Fatalf("App-mode provisioning returned a requeue: %#v", result)
	}
	if minter.calls == 0 {
		t.Fatalf("LaunchSecure never minted the run's installation token")
	}
	secret := &corev1.Secret{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}, secret); err != nil {
		t.Fatal(err)
	}
	if len(secret.Data) != 1 || string(secret.Data[topology.CredKeyInstallationToken]) == "" {
		t.Fatalf("App-mode credential Secret keys = %v", secret.Data)
	}
}

func TestInstallationTokenRequeueDelay(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-delay")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID: string(run.UID), ProviderConfigRef: "github",
		ProviderEndpoint: "https://api.github.com/", CredentialRefDigest: registration.Projection().Digest,
		BaseRepo: "Acme/Widgets", WorkRepo: "Acme/Widgets",
	}
	minter := &fixedInstallationTokenMinter{token: "delay-token", expiresAt: now.Add(2 * time.Hour)}
	control, c := secureControl(t, run)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }

	// No document yet: the next pass is the churn delay, not a deadline.
	if delay := control.InstallationTokenRequeueDelay(context.Background(), run); delay != secureRequeueDelay {
		t.Fatalf("delay without a document = %v, want %v", delay, secureRequeueDelay)
	}
	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	// A token 2h out with a 25m refresh lead is refreshed in ~95m.
	if delay := control.InstallationTokenRequeueDelay(context.Background(), run); delay != 95*time.Minute {
		t.Fatalf("delay for a fresh token = %v, want 95m", delay)
	}
	// Inside the window the next pass is immediate churn, which mints.
	expired := &corev1.Secret{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}
	if err := c.Get(context.Background(), key, expired); err != nil {
		t.Fatal(err)
	}
	document := topology.InstallationTokenDocument{
		Version: topology.InstallationTokenDocumentVersion, Token: "stale", ExpiresAt: now.Add(10 * time.Minute),
		Owner: "Acme", Endpoint: "https://api.github.com/",
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	expired.Data = map[string][]byte{topology.CredKeyInstallationToken: raw}
	if err := c.Update(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	if delay := control.InstallationTokenRequeueDelay(context.Background(), run); delay != secureRequeueDelay {
		t.Fatalf("delay inside the refresh window = %v, want %v", delay, secureRequeueDelay)
	}
}

func TestInstallationTokenMintIdentityIsProvenBeforeMinting(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		owner    string
		endpoint string
	}{
		{"foreign owner", "Other", "https://api.github.com/"},
		{"endpoint mismatch", "Acme", "https://ghe.example.com/api/v3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := secureRun("installation-identity")
			registration, err := secureRegistry().Select(run.Spec.Repo)
			if err != nil {
				t.Fatal(err)
			}
			run.Status.PublicationPolicy = &courier.PublicationPolicy{
				RunUID: string(run.UID), ProviderConfigRef: "github",
				ProviderEndpoint: registration.Endpoint, CredentialRefDigest: registration.Projection().Digest,
				BaseRepo: "Acme/Widgets", WorkRepo: "Acme/Widgets",
			}
			minter := &fixedInstallationTokenMinter{
				token: "unused", expiresAt: now.Add(2 * time.Hour), owner: tc.owner, endpoint: tc.endpoint,
			}
			control, c := secureControl(t, run)
			control.Config.InstallationTokenMinter = minter
			control.Config.Now = func() time.Time { return now }
			err = control.ensureCredentialsCopy(context.Background(), run)
			permanent := (*SecureNeedsHumanError)(nil)
			if !errors.As(err, &permanent) {
				t.Fatalf("identity mismatch err = %v, want a needs-human verdict", err)
			}
			if minter.calls != 0 {
				t.Fatalf("identity mismatch minted %d tokens", minter.calls)
			}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("identity mismatch wrote a credential Secret: %v", err)
			}
		})
	}
}

func TestInstallationTokenRejectsShortMint(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-short-mint")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID: string(run.UID), ProviderConfigRef: "github",
		ProviderEndpoint: registration.Endpoint, CredentialRefDigest: registration.Projection().Digest,
		BaseRepo: "Acme/Widgets", WorkRepo: "Acme/Widgets",
	}
	// Ten minutes of life is below the refresh lead plus the broker's own
	// margin: accepting it would turn every reconcile into a mint and hand
	// the broker a token it refuses.
	minter := &fixedInstallationTokenMinter{token: "short-token", expiresAt: now.Add(10 * time.Minute)}
	control, c := secureControl(t, run)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }
	if err := control.ensureCredentialsCopy(context.Background(), run); err == nil || strings.Contains(err.Error(), "needs-human") {
		t.Fatalf("short mint err = %v, want a retryable refusal", err)
	}
	if minter.calls != 1 {
		t.Fatalf("short mint calls = %d, want 1", minter.calls)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("short mint wrote a credential Secret: %v", err)
	}
}
