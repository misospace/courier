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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/misospace/courier/internal/topology"
)

const sentinelInstallationToken = "ghs_SENTINEL_DO_NOT_LEAK"

type installationTokenSourceFunc func(context.Context, string) (string, time.Time, error)

func (f installationTokenSourceFunc) InstallationToken(ctx context.Context, repository string) (string, time.Time, error) {
	return f(ctx, repository)
}

func installationDocument(now time.Time) topology.InstallationTokenDocument {
	return topology.InstallationTokenDocument{
		Version:  topology.InstallationTokenDocumentVersion,
		Owner:    "ExampleOrg",
		Endpoint: "https://api.github.com/",
		Entries: []topology.InstallationTokenEntry{
			{Repository: "ExampleOrg/Repo", Token: sentinelInstallationToken, ExpiresAt: now.Add(time.Hour), ReadToken: "read-" + sentinelInstallationToken, ReadExpiresAt: now.Add(time.Hour)},
		},
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

func setInstallationDocument(t *testing.T, reader ctrlclient.Client, document topology.InstallationTokenDocument) {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	if err := reader.Get(context.Background(), ctrlclient.ObjectKey{Namespace: "runs", Name: "run-credentials"}, secret); err != nil {
		t.Fatal(err)
	}
	secret.Data = map[string][]byte{topology.CredKeyInstallationToken: data}
	if err := reader.Update(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
}

func newSourceForTest(t *testing.T, reader ctrlclient.Reader, expectedOwner string, now func() time.Time) *SecretInstallationTokenSource {
	t.Helper()
	source, err := NewSecretInstallationTokenSource(reader, "runs", "run-credentials", topology.CredKeyInstallationToken, expectedOwner, "https://api.github.com", now)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func TestSecretInstallationTokenSourceProfiles(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	document := installationDocument(now)
	source := newSourceForTest(t, installationSecretReader(t, document).Build(), "ExampleOrg", func() time.Time { return now })
	writeToken, writeExpiry, err := source.InstallationToken(context.Background(), "ExampleOrg/Repo")
	if err != nil {
		t.Fatal(err)
	}
	readToken, readExpiry, err := source.ReadProfile().InstallationToken(context.Background(), "ExampleOrg/Repo")
	if err != nil {
		t.Fatal(err)
	}
	if writeToken != sentinelInstallationToken || readToken != document.Entries[0].ReadToken || readToken == writeToken || !writeExpiry.Equal(document.Entries[0].ExpiresAt) || !readExpiry.Equal(document.Entries[0].ReadExpiresAt) {
		t.Fatalf("write profile = %q %v, read profile = %q %v", writeToken, writeExpiry, readToken, readExpiry)
	}
	// ReadProfile shares no mutable state: the original source still serves
	// the write-capable profile after the copy was taken.
	againToken, _, err := source.InstallationToken(context.Background(), "ExampleOrg/Repo")
	if err != nil || againToken != sentinelInstallationToken {
		t.Fatalf("write profile after ReadProfile() = %q, %v", againToken, err)
	}
	for _, edit := range []func(*topology.InstallationTokenEntry){
		func(entry *topology.InstallationTokenEntry) { entry.ReadToken = "" },
		func(entry *topology.InstallationTokenEntry) {
			entry.ReadExpiresAt = now.Add(2*time.Minute - time.Second)
		},
	} {
		invalid := installationDocument(now)
		edit(&invalid.Entries[0])
		readSource := newSourceForTest(t, installationSecretReader(t, invalid).Build(), "ExampleOrg", func() time.Time { return now }).ReadProfile()
		if token, expiry, err := readSource.InstallationToken(context.Background(), "ExampleOrg/Repo"); err == nil || token != "" || !expiry.IsZero() {
			t.Fatalf("invalid read profile served %q, %v, %v", token, expiry, err)
		}
	}
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
		{name: "empty token", edit: func(d *topology.InstallationTokenDocument) { d.Entries[0].Token = "" }},
		{name: "owner mismatch", edit: func(d *topology.InstallationTokenDocument) { d.Owner = "other" }},
		{name: "endpoint mismatch", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://evil.invalid/" }},
		{name: "insecure endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "http://api.github.com/" }},
		{name: "userinfo endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://user@api.github.com/" }},
		{name: "query endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://api.github.com/?token=x" }},
		{name: "fragment endpoint", edit: func(d *topology.InstallationTokenDocument) { d.Endpoint = "https://api.github.com/#x" }},
		{name: "expired", edit: func(d *topology.InstallationTokenDocument) { d.Entries[0].ExpiresAt = now }},
		{name: "inside expiry margin", edit: func(d *topology.InstallationTokenDocument) {
			d.Entries[0].ExpiresAt = now.Add(2*time.Minute - time.Second)
		}},
		{name: "entry for another repository only", edit: func(d *topology.InstallationTokenDocument) {
			d.Entries = []topology.InstallationTokenEntry{{Repository: "ExampleOrg/Other", Token: "other-" + sentinelInstallationToken, ExpiresAt: now.Add(time.Hour)}}
		}},
		{name: "no entries", edit: func(d *topology.InstallationTokenDocument) { d.Entries = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := valid
			doc.Entries = append([]topology.InstallationTokenEntry(nil), valid.Entries...)
			if tt.edit != nil {
				tt.edit(&doc)
			}
			source := newSourceForTest(t, installationSecretReader(t, doc).Build(), "exampleorg", func() time.Time { return now })
			got, expiry, err := source.InstallationToken(context.Background(), "exampleorg/repo")
			if tt.wantOK {
				if err != nil || got != sentinelInstallationToken || !expiry.Equal(doc.Entries[0].ExpiresAt) {
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
	base := string(data)
	shapes := map[string]string{
		"unknown field":       strings.TrimSuffix(base, "}") + `,"futureField":"x"}`,
		"unknown entry field": strings.Replace(base, `"token"`, `"futureField":"x","token"`, 1),
		"trailing bytes":      base + `{"version":1}`,
		"trailing bracket":    base + "}",
	}
	for name, content := range shapes {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: "runs", Name: "run-credentials"},
				Data:       map[string][]byte{topology.CredKeyInstallationToken: []byte(content)},
			}).Build()
			source := newSourceForTest(t, reader, "ExampleOrg", func() time.Time { return now })
			got, expiry, err := source.InstallationToken(context.Background(), "exampleorg/repo")
			if err == nil || got != "" || expiry != (time.Time{}) {
				t.Fatalf("%s document returned %q, %v, %v", name, got, expiry, err)
			}
			if strings.Contains(err.Error(), sentinelInstallationToken) {
				t.Fatal("parse error leaked installation token")
			}
		})
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
	_, _, err = source.InstallationToken(context.Background(), "owner/repo")
	if err == nil || strings.Contains(err.Error(), sentinelInstallationToken) {
		t.Fatalf("missing secret error = %v", err)
	}
	if _, _, err := source.InstallationToken(context.Background(), ""); err == nil {
		t.Fatal("empty repository accepted")
	}
}

func TestSecretInstallationTokenSourceRevalidatesBindingOnEveryCall(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	doc := installationDocument(now)
	reader := installationSecretReader(t, doc).Build()
	source := newSourceForTest(t, reader, "ExampleOrg", func() time.Time { return now })
	if got, _, err := source.InstallationToken(context.Background(), "exampleorg/repo"); err != nil || got != sentinelInstallationToken {
		t.Fatalf("first InstallationToken() = %q, %v", got, err)
	}
	refuses := func(what string) {
		t.Helper()
		got, expiry, err := source.InstallationToken(context.Background(), "exampleorg/repo")
		if err == nil || got != "" || expiry != (time.Time{}) {
			t.Fatalf("%s served %q, %v, %v", what, got, expiry, err)
		}
		if strings.Contains(err.Error(), sentinelInstallationToken) {
			t.Fatalf("%s error leaked installation token", what)
		}
	}

	rotated := installationDocument(now)
	rotated.Entries[0].Token = "rotated-" + sentinelInstallationToken
	setInstallationDocument(t, reader, rotated)
	if got, _, err := source.InstallationToken(context.Background(), "exampleorg/repo"); err != nil || got != "rotated-"+sentinelInstallationToken {
		t.Fatalf("rotated InstallationToken() = %q, %v", got, err)
	}

	drifted := installationDocument(now)
	drifted.Entries[0].Token = "drift-" + sentinelInstallationToken
	drifted.Owner = "evil"
	setInstallationDocument(t, reader, drifted)
	refuses("owner drift")

	drifted.Owner = "ExampleOrg"
	drifted.Endpoint = "https://evil.invalid"
	setInstallationDocument(t, reader, drifted)
	refuses("endpoint drift")

	drifted.Endpoint = "https://api.github.com/"
	drifted.Entries = []topology.InstallationTokenEntry{
		{Repository: "ExampleOrg/Elsewhere", Token: "elsewhere-" + sentinelInstallationToken, ExpiresAt: now.Add(time.Hour)},
	}
	setInstallationDocument(t, reader, drifted)
	refuses("coverage drift")

	setInstallationDocument(t, reader, doc)
	now = doc.Entries[0].ExpiresAt.Add(-2 * time.Minute)
	refuses("expiry boundary")
}

func TestSecretInstallationTokenSourceServesOnlyTheRequestedRepositoryToken(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	doc := topology.InstallationTokenDocument{
		Version:  topology.InstallationTokenDocumentVersion,
		Owner:    "acme",
		Endpoint: "https://api.github.com/",
		Entries: []topology.InstallationTokenEntry{
			{Repository: "acme/demo", Token: "base-token", ExpiresAt: now.Add(2 * time.Hour)},
			{Repository: "acme/demo-work", Token: "work-token", ExpiresAt: now.Add(90 * time.Minute)},
		},
	}
	source := newSourceForTest(t, installationSecretReader(t, doc).Build(), "ACME", func() time.Time { return now })
	if got, expiry, err := source.InstallationToken(context.Background(), "acme/demo"); err != nil || got != "base-token" || !expiry.Equal(doc.Entries[0].ExpiresAt) {
		t.Fatalf("base ask = %q, %v, %v; want the base entry only", got, expiry, err)
	}
	if got, expiry, err := source.InstallationToken(context.Background(), "ACME/DEMO-WORK"); err != nil || got != "work-token" || !expiry.Equal(doc.Entries[1].ExpiresAt) {
		t.Fatalf("work ask = %q, %v, %v; want the work entry only", got, expiry, err)
	}
	for _, repository := range []string{"acme/other", "other/demo", "acme", "demo", "acme/demo/extra"} {
		got, expiry, err := source.InstallationToken(context.Background(), repository)
		if err == nil || got != "" || expiry != (time.Time{}) {
			t.Fatalf("InstallationToken(%q) = %q, %v, %v; want refusal without any token", repository, got, expiry, err)
		}
		if strings.Contains(err.Error(), "base-token") || strings.Contains(err.Error(), "work-token") {
			t.Fatal("refusal leaked an installation token")
		}
	}
}

func TestSecretInstallationTokenSourceBindingRefusalsServeNoRepository(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	documents := map[string]topology.InstallationTokenDocument{
		"owner mismatch": {
			Version: topology.InstallationTokenDocumentVersion, Owner: "evil", Endpoint: "https://api.github.com",
			Entries: []topology.InstallationTokenEntry{{Repository: "acme/demo", Token: "base-token", ExpiresAt: now.Add(time.Hour)}},
		},
		"endpoint mismatch": {
			Version: topology.InstallationTokenDocumentVersion, Owner: "acme", Endpoint: "https://evil.invalid",
			Entries: []topology.InstallationTokenEntry{{Repository: "acme/demo", Token: "base-token", ExpiresAt: now.Add(time.Hour)}},
		},
		"wrong version": {
			Version: topology.InstallationTokenDocumentVersion + 1, Owner: "acme", Endpoint: "https://api.github.com",
			Entries: []topology.InstallationTokenEntry{{Repository: "acme/demo", Token: "base-token", ExpiresAt: now.Add(time.Hour)}},
		},
	}
	for name, doc := range documents {
		t.Run(name, func(t *testing.T) {
			source := newSourceForTest(t, installationSecretReader(t, doc).Build(), "acme", func() time.Time { return now })
			for _, repository := range []string{"acme/demo", "acme/demo-work"} {
				if got, expiry, err := source.InstallationToken(context.Background(), repository); err == nil || got != "" || expiry != (time.Time{}) {
					t.Fatalf("%s document served %q = %q, %v, %v", name, repository, got, expiry, err)
				}
			}
		})
	}
}

func TestSecretInstallationTokenSourceNearExpiryRefusesOnlyThatRepository(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	doc := topology.InstallationTokenDocument{
		Version:  topology.InstallationTokenDocumentVersion,
		Owner:    "acme",
		Endpoint: "https://api.github.com",
		Entries: []topology.InstallationTokenEntry{
			{Repository: "acme/demo", Token: "base-token", ExpiresAt: now.Add(time.Minute)},
			{Repository: "acme/demo-work", Token: "work-token", ExpiresAt: now.Add(2 * time.Hour)},
		},
	}
	source := newSourceForTest(t, installationSecretReader(t, doc).Build(), "acme", func() time.Time { return now })
	if got, expiry, err := source.InstallationToken(context.Background(), "acme/demo"); err == nil || got != "" || expiry != (time.Time{}) {
		t.Fatalf("near-expiry base entry served %q, %v, %v", got, expiry, err)
	}
	if got, _, err := source.InstallationToken(context.Background(), "acme/demo-work"); err != nil || got != "work-token" {
		t.Fatalf("healthy work entry = %q, %v; want its own token despite the stale base entry", got, err)
	}
}

func TestInstallationGitCredentialsUsesSourceEveryCall(t *testing.T) {
	calls := 0
	var asked []string
	source := installationTokenSourceFunc(func(_ context.Context, repository string) (string, time.Time, error) {
		calls++
		asked = append(asked, repository)
		return "rotated-token", time.Now().Add(time.Hour), nil
	})
	credentials := NewInstallationGitCredentials(source, []string{"org/repo"})
	for range 2 {
		got, err := credentials.Credentials(context.Background(), "Org/Repo")
		if err != nil || got.Username != topology.DefaultGitUsername || got.Token != "rotated-token" {
			t.Fatalf("Credentials() = %+v, %v", got, err)
		}
	}
	if calls != 2 {
		t.Fatalf("source calls = %d, want 2", calls)
	}
	for _, repository := range asked {
		if repository != "org/repo" {
			t.Fatalf("source asked with %q, want the canonical pinned identity org/repo", repository)
		}
	}
}

func TestInstallationGitCredentialsServesOnlyBoundRepositories(t *testing.T) {
	tokens := map[string]string{"org/base": "base-token", "Org/Work": "work-token"}
	var asked []string
	source := installationTokenSourceFunc(func(_ context.Context, repository string) (string, time.Time, error) {
		asked = append(asked, repository)
		token, ok := tokens[repository]
		if !ok {
			return "", time.Time{}, errors.New("source asked for an identity it never minted: " + repository)
		}
		return token, time.Now().Add(time.Hour), nil
	})
	credentials := NewInstallationGitCredentials(source, []string{"org/base", "", "Org/Work"})
	for _, tt := range []struct{ asked, wantToken, wantIdentity string }{
		{asked: "org/base", wantToken: "base-token", wantIdentity: "org/base"},
		{asked: "ORG/BASE", wantToken: "base-token", wantIdentity: "org/base"},
		{asked: "org/work", wantToken: "work-token", wantIdentity: "Org/Work"},
		{asked: "ORG/WORK", wantToken: "work-token", wantIdentity: "Org/Work"},
	} {
		got, err := credentials.Credentials(context.Background(), tt.asked)
		if err != nil || got.Token != tt.wantToken || got.Username != topology.DefaultGitUsername {
			t.Fatalf("Credentials(%q) = %+v, %v", tt.asked, got, err)
		}
		if asked[len(asked)-1] != tt.wantIdentity {
			t.Fatalf("Credentials(%q) asked the source for %q, want the canonical pinned identity %q", tt.asked, asked[len(asked)-1], tt.wantIdentity)
		}
	}
}

func TestInstallationGitCredentialsRefusesUnboundRepositories(t *testing.T) {
	for _, tt := range []struct {
		name  string
		bound []string
		asked string
	}{
		{name: "other owner", bound: []string{"org/repo"}, asked: "other/repo"},
		{name: "other name", bound: []string{"org/repo"}, asked: "org/other"},
		{name: "path extension", bound: []string{"org/repo"}, asked: "org/repo/extra"},
		{name: "unbound provider", bound: nil, asked: "org/repo"},
		{name: "empty entries only", bound: []string{""}, asked: "org/repo"},
		{name: "empty ask", bound: []string{"org/repo"}, asked: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			credentials := NewInstallationGitCredentials(installationTokenSourceFunc(func(_ context.Context, _ string) (string, time.Time, error) {
				calls++
				return sentinelInstallationToken, time.Now().Add(time.Hour), nil
			}), tt.bound)
			got, err := credentials.Credentials(context.Background(), tt.asked)
			if err == nil || got != (GitCredentials{}) || calls != 0 {
				t.Fatalf("Credentials(%q) bound to %v returned %+v, %v (source calls %d)", tt.asked, tt.bound, got, err, calls)
			}
			if strings.Contains(err.Error(), sentinelInstallationToken) {
				t.Fatal("refusal leaked installation token")
			}
		})
	}
}

func TestInstallationGitCredentialsDoesNotLeakFailures(t *testing.T) {
	credentialSource := NewInstallationGitCredentials(installationTokenSourceFunc(func(_ context.Context, _ string) (string, time.Time, error) {
		return "", time.Time{}, errors.New("failed with " + sentinelInstallationToken)
	}), []string{"org/repo"})
	_, err := credentialSource.Credentials(context.Background(), "org/repo")
	if err == nil || strings.Contains(err.Error(), sentinelInstallationToken) {
		t.Fatalf("credential error = %v", err)
	}
}
