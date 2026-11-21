package broker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/misospace/courier/internal/topology"
)

const sentinelInstallationToken = "ghs_SENTINEL_DO_NOT_LEAK"

type installationTokenSourceFunc func(context.Context) (string, time.Time, error)

func (f installationTokenSourceFunc) InstallationToken(ctx context.Context) (string, time.Time, error) {
	return f(ctx)
}

func installationDocument(now time.Time) topology.InstallationTokenDocument {
	return topology.InstallationTokenDocument{
		Version: topology.InstallationTokenDocumentVersion, Token: sentinelInstallationToken,
		ExpiresAt: now.Add(time.Hour), Owner: "ExampleOrg", Endpoint: "https://api.github.com/",
	}
}

func installationSecretReader(t *testing.T, document topology.InstallationTokenDocument) *fake.ClientBuilder {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "runs", Name: "run-credentials"},
		Data:       map[string][]byte{topology.CredKeyInstallationToken: data},
	})
}

func TestSecretInstallationTokenSourceValidation(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	valid := installationDocument(now)
	tests := []struct {
		name   string
		edit   func(*topology.InstallationTokenDocument)
		wantOK bool
	}{
		{name: "valid", wantOK: true},
		{name: "wrong version", edit: func(d *topology.InstallationTokenDocument) { d.Version++ }},
		{name: "empty token", edit: func(d *topology.InstallationTokenDocument) { d.Token = "" }},
		{name: "owner mismatch", edit: func(d *topology.InstallationTokenDocument) { d.Owner = "other" }},
		{name: "endpoint mismatch", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://evil.invalid/" }},
		{name: "insecure endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "http://api.github.com/" }},
		{name: "userinfo endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://user@api.github.com/" }},
		{name: "query endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://api.github.com/?token=x" }},
		{name: "fragment endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://api.github.com/#x" }},
		{name: "expired", edit: func(d *topology.InstallationTokenDocument) { d.ExpiresAt = now }},
		{name: "inside expiry margin", edit: func(d *topology.InstallationTokenDocument) { d.ExpiresAt = now.Add(2*time.Minute - time.Second) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := valid
			if tt.edit != nil {
				tt.edit(&doc)
			}
			reader := installationSecretReader(t, doc).Build()
			source, err := NewSecretInstallationTokenSource(reader, "runs", "run-credentials", topology.CredKeyInstallationToken, "exampleorg", "https://api.github.com", func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			got, expiry, err := source.InstallationToken(context.Background())
			if tt.wantOK {
				if err != nil || got != sentinelInstallationToken || !expiry.Equal(doc.ExpiresAt) {
					t.Fatalf("InstallationToken() = %q, %v, %v", got, expiry, err)
				}
				return
			}
			if err == nil || got != "" || expiry != (time.Time{}) {
				t.Fatalf("invalid document returned %q, %v, %v", got, expiry, err)
			}
			if strings.Contains(err.Error(), sentinelInstallationToken) {
				t.Fatal("error leaked installation token")
			}
		})
	}
}

func TestSecretInstallationTokenSourceRejectsUnknownShapes(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	document := installationDocument(now)
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	unknown := strings.TrimSuffix(string(data), "}") + `,"futureField":"x"}`
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "runs", Name: "run-credentials"},
		Data:       map[string][]byte{topology.CredKeyInstallationToken: []byte(unknown)},
	}).Build()
	source, err := NewSecretInstallationTokenSource(reader, "runs", "run-credentials", topology.CredKeyInstallationToken, "ExampleOrg", "https://api.github.com", func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	got, expiry, err := source.InstallationToken(context.Background())
	if err == nil || got != "" || expiry != (time.Time{}) {
		t.Fatalf("unknown-field document returned %q, %v, %v", got, expiry, err)
	}
	if strings.Contains(err.Error(), sentinelInstallationToken) {
		t.Fatal("parse error leaked installation token")
	}
}

func TestSecretInstallationTokenSourceConstructorAndUnavailableSecret(t *testing.T) {
	if _, err := NewSecretInstallationTokenSource(nil, "runs", "secret", "key", "owner", "https://api.github.com", nil); err == nil {
		t.Fatal("accepted nil reader")
	}
	if _, err := NewSecretInstallationTokenSource(nil, "", "", "", "", "", nil); err == nil {
		t.Fatal("accepted empty expectations")
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	source, err := NewSecretInstallationTokenSource(fake.NewClientBuilder().WithScheme(scheme).Build(), "runs", "missing", "key", "owner", "https://api.github.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = source.InstallationToken(context.Background())
	if err == nil || strings.Contains(err.Error(), sentinelInstallationToken) {
		t.Fatalf("missing secret error = %v", err)
	}
}

func TestSecretInstallationTokenSourceCacheAndRefresh(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	doc := installationDocument(now)
	reader := installationSecretReader(t, doc).Build()
	source, err := NewSecretInstallationTokenSource(reader, "runs", "run-credentials", topology.CredKeyInstallationToken, "ExampleOrg", "https://api.github.com", func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, _, err := source.InstallationToken(context.Background())
		if err != nil || got != sentinelInstallationToken {
			t.Fatalf("cached token = %q, %v", got, err)
		}
	}
	now = doc.ExpiresAt.Add(-2 * time.Minute)
	if _, _, err := source.InstallationToken(context.Background()); err == nil {
		t.Fatal("used cached token at its expiry safety boundary")
	} else if strings.Contains(err.Error(), sentinelInstallationToken) {
		t.Fatal("refresh error leaked installation token")
	}
}

func TestInstallationGitCredentialsUsesSourceEveryCall(t *testing.T) {
	calls := 0
	source := installationTokenSourceFunc(func(context.Context) (string, time.Time, error) {
		calls++
		return "rotated-token", time.Now().Add(time.Hour), nil
	})
	credentials := NewInstallationGitCredentials(source, "org/repo")
	for range 2 {
		got, err := credentials.Credentials(context.Background(), "Org/Repo")
		if err != nil || got.Username != topology.DefaultGitUsername || got.Token != "rotated-token" {
			t.Fatalf("Credentials() = %+v, %v", got, err)
		}
	}
	if calls != 2 {
		t.Fatalf("source calls = %d, want 2", calls)
	}
}

func TestInstallationGitCredentialsServesOnlyBoundRepository(t *testing.T) {
	for _, tt := range []struct{ name, bound, asked string }{
		{name: "other owner", bound: "org/repo", asked: "other/repo"},
		{name: "other name", bound: "org/repo", asked: "org/other"},
		{name: "path extension", bound: "org/repo", asked: "org/repo/extra"},
		{name: "unbound provider", bound: "", asked: "org/repo"},
		{name: "empty ask", bound: "org/repo", asked: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			credentials := NewInstallationGitCredentials(installationTokenSourceFunc(func(context.Context) (string, time.Time, error) {
				calls++
				return sentinelInstallationToken, time.Now().Add(time.Hour), nil
			}), tt.bound)
			got, err := credentials.Credentials(context.Background(), tt.asked)
			if err == nil || got != (GitCredentials{}) || calls != 0 {
				t.Fatalf("Credentials(%q) bound to %q returned %+v, %v (source calls %d)", tt.asked, tt.bound, got, err, calls)
			}
			if strings.Contains(err.Error(), sentinelInstallationToken) {
				t.Fatal("refusal leaked installation token")
			}
		})
	}
}

func TestInstallationGitCredentialsDoesNotLeakFailures(t *testing.T) {
	credentialSource := NewInstallationGitCredentials(installationTokenSourceFunc(func(context.Context) (string, time.Time, error) {
		return "", time.Time{}, errors.New("failed with " + sentinelInstallationToken)
	}), "org/repo")
	_, err := credentialSource.Credentials(context.Background(), "org/repo")
	if err == nil || strings.Contains(err.Error(), sentinelInstallationToken) {
		t.Fatalf("credential error = %v", err)
	}
}
