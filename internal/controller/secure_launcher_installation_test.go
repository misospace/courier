package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/topology"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type installationMintCall struct {
	repositories []string
	permissions  map[string]string
}

type fixedInstallationTokenMinter struct {
	calls     int
	mints     []installationMintCall
	token     string
	expiresAt time.Time
	err       error
	owner     string
	endpoint  string
}

func (m *fixedInstallationTokenMinter) Identity() (string, string) {
	if m.owner != "" || m.endpoint != "" {
		return m.owner, m.endpoint
	}
	return "Acme", "https://api.github.com/"
}

func (m *fixedInstallationTokenMinter) Mint(_ context.Context, repositories []string, permissions map[string]string) (string, time.Time, error) {
	m.calls++
	m.mints = append(m.mints, installationMintCall{
		repositories: append([]string(nil), repositories...),
		permissions:  permissions,
	})
	if m.err != nil {
		return "", time.Time{}, m.err
	}
	return fmt.Sprintf("%s-%d", m.token, m.calls), m.expiresAt, nil
}

func installationPolicy(run *courier.CoderRun, registration *forge.Registration, baseRepo, workRepo string) *courier.PublicationPolicy {
	return &courier.PublicationPolicy{
		RunUID: string(run.UID), ProviderConfigRef: "github", ProviderEndpoint: "https://api.github.com/",
		CredentialRefDigest: registration.Projection().Digest,
		BaseRepo:            baseRepo, WorkRepo: workRepo,
	}
}

func controllerOwnerRef(name string, uid types.UID) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: "courier.misospace.dev/v1alpha1", Kind: "CoderRun",
		Name: name, UID: uid, Controller: &controller,
	}
}

func installationSecret(run *courier.CoderRun, raw []byte, ownerRefs ...metav1.OwnerReference) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name),
			OwnerReferences: ownerRefs,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{topology.CredKeyInstallationToken: raw},
	}
}

func readInstallationSecret(t *testing.T, c client.Client, run *courier.CoderRun) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}
	if err := c.Get(context.Background(), key, secret); err != nil {
		t.Fatal(err)
	}
	return secret
}

func decodeInstallationDocument(t *testing.T, raw []byte) topology.InstallationTokenDocument {
	t.Helper()
	var document topology.InstallationTokenDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("unmarshal installation-token document: %v", err)
	}
	return document
}

func TestEnsureInstallationTokenCredential(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-token")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	policy := installationPolicy(run, registration, "Acme/Widgets", "Acme/Widgets")
	run.Status.PublicationPolicy = policy
	minter := &fixedInstallationTokenMinter{token: "initial-token", expiresAt: now.Add(2 * time.Hour)}
	control, c := secureControl(t, run)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }

	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatalf("first ensureCredentialsCopy: %v", err)
	}
	secret := readInstallationSecret(t, c, run)
	if len(secret.Data) != 1 {
		t.Fatalf("credential Secret keys = %v, want only installation token", secret.Data)
	}
	document := decodeInstallationDocument(t, secret.Data[topology.CredKeyInstallationToken])
	if document.Version != topology.InstallationTokenDocumentVersion ||
		document.Owner != "Acme" || document.Endpoint != "https://api.github.com/" || len(document.Entries) != 1 {
		t.Fatalf("installation token document = %+v", document)
	}
	entry, ok := document.Entry(policy.BaseRepo)
	if !ok || entry.Token != "initial-token-1" || entry.ExpiresAt != minter.expiresAt || entry.ReadToken != "initial-token-2" || entry.ReadExpiresAt != minter.expiresAt {
		t.Fatalf("installation token entry = %+v (found %v)", entry, ok)
	}
	// A same-repository run is the one case where both capability classes
	// genuinely target one repository: exactly one mint with the merged
	// profile — the base profile's writes and reads plus contents write.
	if minter.calls != 2 || len(minter.mints) != 2 {
		t.Fatalf("mint calls = %d mints %+v, want 2", minter.calls, minter.mints)
	}
	if len(minter.mints[0].repositories) != 1 || minter.mints[0].repositories[0] != policy.BaseRepo {
		t.Fatalf("mint repositories = %v", minter.mints[0].repositories)
	}
	wantMerged := map[string]string{
		"administration": "read", "checks": "read", "contents": "write",
		"issues": "write", "metadata": "read", "pull_requests": "write", "statuses": "read",
	}
	if !reflect.DeepEqual(minter.mints[0].permissions, wantMerged) {
		t.Fatalf("same-repo merged permissions = %+v, want %+v", minter.mints[0].permissions, wantMerged)
	}
	wantRead := map[string]string{
		"checks": "read", "contents": "read", "issues": "read",
		"metadata": "read", "pull_requests": "read",
	}
	if !reflect.DeepEqual(minter.mints[1].permissions, wantRead) || reflect.DeepEqual(minter.mints[0].permissions, minter.mints[1].permissions) {
		t.Fatalf("read permissions = %+v, want distinct all-read profile %+v", minter.mints[1].permissions, wantRead)
	}
	for _, level := range minter.mints[1].permissions {
		if level == "write" {
			t.Fatalf("read profile contains write permission: %+v", minter.mints[1].permissions)
		}
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
	if minter.calls != 2 {
		t.Fatalf("fresh credential caused %d mints, want 2", minter.calls)
	}

	now = entry.ExpiresAt.Add(-installationTokenRefreshLead + time.Minute)
	control.Config.Now = func() time.Time { return now }
	minter.token = "rotated-token"
	minter.expiresAt = now.Add(2 * time.Hour)
	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatalf("refresh ensureCredentialsCopy: %v", err)
	}
	if minter.calls != 4 {
		t.Fatalf("refresh mint calls = %d, want 4", minter.calls)
	}
	secret = readInstallationSecret(t, c, run)
	document = decodeInstallationDocument(t, secret.Data[topology.CredKeyInstallationToken])
	if entry, ok := document.Entry(policy.BaseRepo); !ok || entry.Token != "rotated-token-3" || entry.ReadToken != "rotated-token-4" || len(secret.Data) != 1 {
		t.Fatalf("rotated credential document = %+v; keys %v", document, secret.Data)
	}
}

func TestEnsureInstallationTokenMintsPerRepository(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-two-repos")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	policy := installationPolicy(run, registration, "Acme/Widgets", "Acme/Gadgets")
	run.Status.PublicationPolicy = policy
	minter := &fixedInstallationTokenMinter{token: "scoped-token", expiresAt: now.Add(2 * time.Hour)}
	control, c := secureControl(t, run)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }

	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatalf("ensureCredentialsCopy: %v", err)
	}
	// One token per repository, never one token over both: GitHub applies a
	// token's permissions to every repository it names, so the base token
	// must not gain contents write and the work token must not gain the
	// publication surfaces.
	if len(minter.mints) != 4 {
		t.Fatalf("mint calls = %d mints %+v, want 4", minter.calls, minter.mints)
	}
	wantBase := map[string]string{
		"administration": "read", "checks": "read", "contents": "read",
		"issues": "write", "metadata": "read", "pull_requests": "write", "statuses": "read",
	}
	wantWork := map[string]string{"administration": "read", "contents": "write", "metadata": "read"}
	wantRead := map[string]string{
		"checks": "read", "contents": "read", "issues": "read",
		"metadata": "read", "pull_requests": "read",
	}
	if !reflect.DeepEqual(minter.mints[0].repositories, []string{"Acme/Widgets"}) ||
		!reflect.DeepEqual(minter.mints[0].permissions, wantBase) {
		t.Fatalf("base mint = %+v, want repositories [Acme/Widgets] permissions %+v", minter.mints[0], wantBase)
	}
	if minter.mints[0].permissions["contents"] == "write" {
		t.Fatal("base-repository token carries contents write")
	}
	if !reflect.DeepEqual(minter.mints[1].repositories, []string{"Acme/Widgets"}) || !reflect.DeepEqual(minter.mints[1].permissions, wantRead) || reflect.DeepEqual(minter.mints[0].permissions, minter.mints[1].permissions) {
		t.Fatalf("base read mint = %+v, want repositories [Acme/Widgets] permissions %+v", minter.mints[1], wantRead)
	}
	if !reflect.DeepEqual(minter.mints[2].repositories, []string{"Acme/Gadgets"}) ||
		!reflect.DeepEqual(minter.mints[2].permissions, wantWork) {
		t.Fatalf("work mint = %+v, want repositories [Acme/Gadgets] permissions %+v", minter.mints[2], wantWork)
	}
	if !reflect.DeepEqual(minter.mints[3].repositories, []string{"Acme/Gadgets"}) || !reflect.DeepEqual(minter.mints[3].permissions, wantRead) || reflect.DeepEqual(minter.mints[2].permissions, minter.mints[3].permissions) {
		t.Fatalf("work read mint = %+v, want repositories [Acme/Gadgets] permissions %+v", minter.mints[3], wantRead)
	}
	if _, has := minter.mints[2].permissions["issues"]; has {
		t.Fatal("work-repository token carries issues permission")
	}
	if _, has := minter.mints[2].permissions["pull_requests"]; has {
		t.Fatal("work-repository token carries pull_requests permission")
	}

	secret := readInstallationSecret(t, c, run)
	if len(secret.Data) != 1 {
		t.Fatalf("credential Secret keys = %v, want only installation token", secret.Data)
	}
	document := decodeInstallationDocument(t, secret.Data[topology.CredKeyInstallationToken])
	if document.Owner != "Acme" || document.Endpoint != "https://api.github.com/" || len(document.Entries) != 2 {
		t.Fatalf("document = %+v", document)
	}
	baseEntry, ok := document.Entry("Acme/Widgets")
	if !ok || baseEntry.Token != "scoped-token-1" || baseEntry.ReadToken != "scoped-token-2" || baseEntry.ReadToken == baseEntry.Token {
		t.Fatalf("base entry = %+v (found %v)", baseEntry, ok)
	}
	workEntry, ok := document.Entry("acme/gadgets")
	if !ok || workEntry.Token != "scoped-token-3" || workEntry.ReadToken != "scoped-token-4" || workEntry.ReadToken == workEntry.Token || workEntry.Token == baseEntry.Token {
		t.Fatalf("work entry = %+v (found %v)", workEntry, ok)
	}

	// Fast path: the run-owned Secret already carries a fresh document whose
	// entries cover both pinned repositories.
	if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
		t.Fatalf("fresh ensureCredentialsCopy: %v", err)
	}
	if minter.calls != 4 {
		t.Fatalf("fresh document caused %d mint calls, want 4", minter.calls)
	}
}

func TestEnsureInstallationTokenRejectsForeignSecret(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-foreign")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	run.Status.PublicationPolicy = installationPolicy(run, registration, "Acme/Widgets", "Acme/Widgets")
	foreign := installationSecret(run, []byte("someone-elses-credential"))
	minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour)}
	control, c := secureControl(t, run, foreign)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }

	err = control.ensureCredentialsCopy(context.Background(), run)
	permanent := (*SecureNeedsHumanError)(nil)
	if !errors.As(err, &permanent) || permanent.Reason != "InstallationTokenOwnership" {
		t.Fatalf("foreign Secret err = %v, want needs-human InstallationTokenOwnership", err)
	}
	if minter.calls != 0 {
		t.Fatalf("foreign Secret minted %d tokens", minter.calls)
	}
	if secret := readInstallationSecret(t, c, run); string(secret.Data[topology.CredKeyInstallationToken]) != "someone-elses-credential" {
		t.Fatalf("foreign Secret data was modified: %s", secret.Data[topology.CredKeyInstallationToken])
	}
}

func TestEnsureInstallationTokenRejectsStaleRunOwnerRef(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	run := secureRun("installation-stale-owner")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	run.Status.PublicationPolicy = installationPolicy(run, registration, "Acme/Widgets", "Acme/Widgets")
	// A same-named Secret controlled by a previous run incarnation is foreign
	// to this UID: the deterministic name proves nothing until the controller
	// reference names this exact run.
	stale := installationSecret(run, []byte("prior-run-credential"), controllerOwnerRef(run.Name, types.UID("uid-a-previous-run")))
	minter := &fixedInstallationTokenMinter{token: "unused", expiresAt: now.Add(2 * time.Hour)}
	control, c := secureControl(t, run, stale)
	control.Config.InstallationTokenMinter = minter
	control.Config.Now = func() time.Time { return now }

	err = control.ensureCredentialsCopy(context.Background(), run)
	permanent := (*SecureNeedsHumanError)(nil)
	if !errors.As(err, &permanent) || permanent.Reason != "InstallationTokenOwnership" {
		t.Fatalf("stale ownerRef err = %v, want needs-human InstallationTokenOwnership", err)
	}
	if minter.calls != 0 {
		t.Fatalf("stale ownerRef minted %d tokens", minter.calls)
	}
	if secret := readInstallationSecret(t, c, run); string(secret.Data[topology.CredKeyInstallationToken]) != "prior-run-credential" {
		t.Fatalf("stale-owner Secret data was modified: %s", secret.Data[topology.CredKeyInstallationToken])
	}
}

func TestEnsureInstallationTokenRefreshesIdentityMismatchedDocument(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		owner    string
		endpoint string
	}{
		{"owner drift", "SomeoneElse", "https://api.github.com/"},
		{"endpoint drift", "Acme", "https://ghe.example.com/api/v3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := secureRun("installation-doc-drift")
			registration, err := secureRegistry().Select(run.Spec.Repo)
			if err != nil {
				t.Fatal(err)
			}
			run.Status.PublicationPolicy = installationPolicy(run, registration, "Acme/Widgets", "Acme/Widgets")
			staleDocument := topology.InstallationTokenDocument{
				Version: topology.InstallationTokenDocumentVersion, Owner: tc.owner, Endpoint: tc.endpoint,
				Entries: []topology.InstallationTokenEntry{
					{Repository: "Acme/Widgets", Token: "drifted", ExpiresAt: now.Add(2 * time.Hour)},
				},
			}
			raw, err := json.Marshal(staleDocument)
			if err != nil {
				t.Fatal(err)
			}
			// Run-owned, so it may be overwritten — but a document bound to a
			// different owner or endpoint is never served as this run's
			// credential.
			owned := installationSecret(run, raw, controllerOwnerRef(run.Name, run.UID))
			owned.OwnerReferences = append(owned.OwnerReferences,
				metav1.OwnerReference{APIVersion: "v1", Kind: "Secret", Name: "a-previous-writer"})
			minter := &fixedInstallationTokenMinter{token: "refreshed-token", expiresAt: now.Add(2 * time.Hour)}
			control, c := secureControl(t, run, owned)
			control.Config.InstallationTokenMinter = minter
			control.Config.Now = func() time.Time { return now }

			if err := control.ensureCredentialsCopy(context.Background(), run); err != nil {
				t.Fatalf("ensureCredentialsCopy: %v", err)
			}
			if minter.calls != 2 {
				t.Fatalf("identity-drifted document minted %d tokens, want 2", minter.calls)
			}
			secret := readInstallationSecret(t, c, run)
			document := decodeInstallationDocument(t, secret.Data[topology.CredKeyInstallationToken])
			if document.Owner != "Acme" || document.Endpoint != registration.Endpoint || len(document.Entries) != 1 {
				t.Fatalf("refreshed document = %+v", document)
			}
			if entry, ok := document.Entry("Acme/Widgets"); !ok || entry.Token != "refreshed-token-1" {
				t.Fatalf("refreshed entry = %+v (found %v)", entry, ok)
			}
			// The refresh converges the owner references to the rendered
			// object rather than preserving what a previous writer left.
			if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].UID != run.UID {
				t.Fatalf("refreshed ownerReferences = %+v", secret.OwnerReferences)
			}
		})
	}
}

func TestEnsureInstallationTokenCreateRace(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		work       string
		winner     func(run *courier.CoderRun) *corev1.Secret
		wantErr    string
		wantReason string
		wantMint   int
	}{
		{
			name: "foreign winner",
			winner: func(run *courier.CoderRun) *corev1.Secret {
				return installationSecret(run, []byte("winner-foreign"))
			},
			wantReason: "InstallationTokenOwnership",
		},
		{
			name: "owned winner bound to a different identity",
			winner: func(run *courier.CoderRun) *corev1.Secret {
				document := topology.InstallationTokenDocument{
					Version: topology.InstallationTokenDocumentVersion, Owner: "SomeoneElse",
					Endpoint: "https://api.github.com/",
					Entries: []topology.InstallationTokenEntry{
						{Repository: "SomeoneElse/Widgets", Token: "winner-drifted", ExpiresAt: now.Add(2 * time.Hour)},
					},
				}
				raw, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				return installationSecret(run, raw, controllerOwnerRef(run.Name, run.UID))
			},
			wantErr: "different identity",
		},
		{
			name: "well-bound winner",
			winner: func(run *courier.CoderRun) *corev1.Secret {
				document := topology.InstallationTokenDocument{
					Version: topology.InstallationTokenDocumentVersion, Owner: "Acme",
					Endpoint: "https://api.github.com/",
					Entries: []topology.InstallationTokenEntry{
						{Repository: "Acme/Widgets", Token: "winner-fine", ExpiresAt: now.Add(2 * time.Hour), ReadToken: "winner-read", ReadExpiresAt: now.Add(2 * time.Hour)},
					},
				}
				raw, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				return installationSecret(run, raw, controllerOwnerRef(run.Name, run.UID))
			},
		},
		{
			name: "owned winner missing the work entry",
			work: "Acme/Gadgets",
			winner: func(run *courier.CoderRun) *corev1.Secret {
				document := topology.InstallationTokenDocument{
					Version: topology.InstallationTokenDocumentVersion, Owner: "Acme",
					Endpoint: "https://api.github.com/",
					Entries: []topology.InstallationTokenEntry{
						{Repository: "Acme/Widgets", Token: "winner-partial", ExpiresAt: now.Add(2 * time.Hour)},
					},
				}
				raw, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				return installationSecret(run, raw, controllerOwnerRef(run.Name, run.UID))
			},
			wantErr:  "different identity",
			wantMint: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := secureRun("installation-create-race")
			registration, err := secureRegistry().Select(run.Spec.Repo)
			if err != nil {
				t.Fatal(err)
			}
			work := tc.work
			if work == "" {
				work = "Acme/Widgets"
			}
			run.Status.PublicationPolicy = installationPolicy(run, registration, "Acme/Widgets", work)
			minter := &fixedInstallationTokenMinter{token: "loser-token", expiresAt: now.Add(2 * time.Hour)}

			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{courier.AddToScheme, corev1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			secretName := topology.CredentialsSecretName(run.Name)
			// Stand in for the race: the initial Get reports the Secret
			// missing, the Create collides with a winner the first read did
			// not see, and the winner re-read through the uncached reader
			// finds the object that actually won.
			firstGet := true
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(tc.winner(run)).
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
						return c.Create(ctx, obj, opts...)
					},
				}).Build()
			control := &SecureControl{
				Client:    c,
				APIReader: c,
				Config: SecureConfig{
					RunNamespace:            secureTestNamespace,
					Registry:                secureRegistry(),
					InstallationTokenMinter: minter,
					Now:                     func() time.Time { return now },
				},
			}

			err = control.ensureCredentialsCopy(context.Background(), run)
			switch {
			case tc.wantReason != "":
				permanent := (*SecureNeedsHumanError)(nil)
				if !errors.As(err, &permanent) || permanent.Reason != tc.wantReason {
					t.Fatalf("race err = %v, want needs-human %s", err, tc.wantReason)
				}
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("race err = %v, want containing %q", err, tc.wantErr)
				}
			case err != nil:
				t.Fatalf("race err = %v, want success", err)
			}
			wantMint := tc.wantMint
			if wantMint == 0 {
				wantMint = 2
			}
			if minter.calls != wantMint {
				t.Fatalf("race mint calls = %d, want %d", minter.calls, wantMint)
			}
		})
	}
}

func TestInstallationTokenOwnerMismatchAndStaticRegression(t *testing.T) {
	run := secureRun("installation-owner-mismatch")
	registration, err := secureRegistry().Select(run.Spec.Repo)
	if err != nil {
		t.Fatal(err)
	}
	run.Status.PublicationPolicy = installationPolicy(run, registration, "Acme/Widgets", "Other/Widgets")
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
	staticRun.Status.PublicationPolicy = installationPolicy(staticRun, staticRegistration, "Acme/Widgets", "Acme/Widgets")
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
	if minter.calls != 2 {
		t.Fatalf("LaunchSecure minted %d tokens, want write and read profiles", minter.calls)
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
	run.Status.PublicationPolicy = installationPolicy(run, registration, "Acme/Widgets", "Acme/Widgets")
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

	writeDocument := func(document topology.InstallationTokenDocument) {
		t.Helper()
		raw, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		fresh := readInstallationSecret(t, c, run)
		fresh.Data = map[string][]byte{topology.CredKeyInstallationToken: raw}
		if err := c.Update(context.Background(), fresh); err != nil {
			t.Fatal(err)
		}
		if delay := control.InstallationTokenRequeueDelay(context.Background(), run); delay != secureRequeueDelay {
			t.Fatalf("delay for document %+v = %v, want %v", document, delay, secureRequeueDelay)
		}
	}
	// An unparseable, wrong-version, or entry-less document is no document.
	writeDocument(topology.InstallationTokenDocument{Version: 99, Owner: "Acme", Endpoint: "https://api.github.com/"})
	writeDocument(topology.InstallationTokenDocument{Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/"})
	fresh := readInstallationSecret(t, c, run)
	fresh.Data = map[string][]byte{topology.CredKeyInstallationToken: []byte("not json")}
	if err := c.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if delay := control.InstallationTokenRequeueDelay(context.Background(), run); delay != secureRequeueDelay {
		t.Fatalf("delay for an unparseable document = %v, want %v", delay, secureRequeueDelay)
	}

	// The delay tracks the EARLIEST entry: a second, sooner-expiring entry
	// pulls the refresh pass forward to its window.
	entries := topology.InstallationTokenDocument{
		Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/",
		Entries: []topology.InstallationTokenEntry{
			{Repository: "Acme/Widgets", Token: "later", ExpiresAt: now.Add(2 * time.Hour)},
			{Repository: "Acme/Gadgets", Token: "sooner", ExpiresAt: now.Add(90 * time.Minute)},
		},
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	fresh = readInstallationSecret(t, c, run)
	fresh.Data = map[string][]byte{topology.CredKeyInstallationToken: raw}
	if err := c.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if delay := control.InstallationTokenRequeueDelay(context.Background(), run); delay != 65*time.Minute {
		t.Fatalf("delay for the earliest entry = %v, want 65m", delay)
	}
	entries.Entries[0].ReadExpiresAt = now.Add(80 * time.Minute)
	entries.Entries[0].ReadToken = "read-soonest"
	raw, err = json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	fresh = readInstallationSecret(t, c, run)
	fresh.Data = map[string][]byte{topology.CredKeyInstallationToken: raw}
	if err := c.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if delay := control.InstallationTokenRequeueDelay(context.Background(), run); delay != 55*time.Minute {
		t.Fatalf("delay for earliest read profile = %v, want 55m", delay)
	}

	// Inside the window the next pass is immediate churn, which mints.
	expired := topology.InstallationTokenDocument{
		Version: topology.InstallationTokenDocumentVersion, Owner: "Acme", Endpoint: "https://api.github.com/",
		Entries: []topology.InstallationTokenEntry{
			{Repository: "Acme/Widgets", Token: "stale", ExpiresAt: now.Add(10 * time.Minute)},
		},
	}
	writeDocument(expired)
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
		t.Fatalf("short write mint calls = %d, want 1", minter.calls)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: topology.CredentialsSecretName(run.Name)}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("short mint wrote a credential Secret: %v", err)
	}
}
