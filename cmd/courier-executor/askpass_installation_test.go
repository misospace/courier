package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

func installationTestEnv(overrides map[string]string) func(string) string {
	env := map[string]string{
		executor.EnvGitInstallation:      "1",
		executor.EnvInstallationOwner:    "acme",
		executor.EnvInstallationEndpoint: "https://api.github.com",
		"POD_NAMESPACE":                  "courier-system",
		executor.EnvInstallationSecret:   "run-7-provider-creds",
		"COURIER_REPO":                   "acme/widgets",
		"COURIER_REPO_URL":               "https://github.com/acme/widgets.git",
		executor.EnvAPITokenFile:         "/projected/token",
	}
	for name, value := range overrides {
		env[name] = value
	}
	return func(name string) string { return env[name] }
}

func installationTestDocument(t *testing.T, owner, endpoint string, entries ...topology.InstallationTokenEntry) []byte {
	t.Helper()
	data, err := json.Marshal(topology.InstallationTokenDocument{
		Version: topology.InstallationTokenDocumentVersion, Owner: owner, Endpoint: endpoint, Entries: entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func installationTestSecret(t *testing.T, document []byte) ctrlclient.Object {
	t.Helper()
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "run-7-provider-creds"},
		Data:       map[string][]byte{executor.InstallationTokenDocumentKey: document},
	}
}

func installationTestReader(t *testing.T, document []byte) ctrlclient.Reader {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(installationTestSecret(t, document)).Build()
}

func passwordPrompt(remoteURL string) []string {
	return []string{"Password for '" + remoteURL + "':"}
}

type failReader struct{ t *testing.T }

func (r failReader) Get(context.Context, ctrlclient.ObjectKey, ctrlclient.Object, ...ctrlclient.GetOption) error {
	r.t.Helper()
	r.t.Error("installation reader must not be consulted for an un-pinned identity")
	return errors.New("consulted")
}

func (r failReader) List(context.Context, ctrlclient.ObjectList, ...ctrlclient.ListOption) error {
	r.t.Helper()
	r.t.Error("installation reader must not be consulted for an un-pinned identity")
	return errors.New("consulted")
}

func TestInstallationAskpassBinding(t *testing.T) {
	remote := "https://github.com/acme/widgets.git"
	for _, testCase := range []struct {
		name          string
		env           map[string]string
		args          []string
		wantIdentity  string
		wantUsername  bool
		wantRefusalse bool
	}{
		{name: "exact work password", env: nil, args: passwordPrompt(remote), wantIdentity: "acme/widgets"},
		{name: "username prompt", env: nil, args: []string{"Username for '" + remote + "':"}, wantIdentity: "acme/widgets", wantUsername: true},
		{
			// The path, not the label, must never decide the credential half.
			name: "password prompt for a path containing username",
			env: map[string]string{
				"COURIER_REPO":     "acme/username-rotation",
				"COURIER_REPO_URL": "https://github.com/acme/username-rotation.git",
			},
			args: passwordPrompt("https://github.com/acme/username-rotation.git"), wantIdentity: "acme/username-rotation",
		},
		{
			name: "username prompt for a path containing Username",
			env: map[string]string{
				"COURIER_REPO":     "acme/username-rotation",
				"COURIER_REPO_URL": "https://github.com/acme/username-rotation.git",
			},
			args: []string{"Username for 'https://github.com/acme/username-rotation.git':"}, wantIdentity: "acme/username-rotation", wantUsername: true,
		},
		{name: "case-insensitive host", env: nil, args: passwordPrompt("HTTPS://GitHub.COM/acme/widgets.git"), wantIdentity: "acme/widgets"},
		{name: "no .git suffix", env: nil, args: passwordPrompt("https://github.com/acme/widgets"), wantIdentity: "acme/widgets"},
		{name: "trailing slash", env: nil, args: passwordPrompt("https://github.com/acme/widgets.git/"), wantIdentity: "acme/widgets"},
		{name: "default port dropped", env: map[string]string{"COURIER_REPO_URL": "https://github.com:443/acme/widgets.git"}, args: passwordPrompt(remote), wantIdentity: "acme/widgets"},
		{
			name: "head identity for work remote",
			env: map[string]string{
				"COURIER_HEAD_REPO":     "forks/widgets",
				"COURIER_BASE_REPO_URL": "https://github.com/acme/upstream.git",
			},
			args: passwordPrompt(remote), wantIdentity: "forks/widgets",
		},
		{
			name: "base identity for base remote",
			env: map[string]string{
				"COURIER_HEAD_REPO":     "forks/widgets",
				"COURIER_BASE_REPO_URL": "https://github.com/acme/upstream.git",
			},
			args: passwordPrompt("https://github.com/acme/upstream"), wantIdentity: "acme/widgets",
		},
		{name: "userinfo rejected", env: nil, args: passwordPrompt("https://tok@github.com/acme/widgets.git"), wantRefusalse: true},
		{name: "password-bearing userinfo rejected", env: nil, args: passwordPrompt("https://x-access-token:secret@github.com/acme/widgets.git"), wantRefusalse: true},
		// git echoes the token username it received into the password
		// prompt; that single canonical userinfo must still bind.
		{name: "served token username binds", env: nil, args: passwordPrompt("https://x-access-token@github.com/acme/widgets.git"), wantIdentity: "acme/widgets"},
		{name: "wrong host rejected", env: nil, args: passwordPrompt("https://evil.example/acme/widgets.git"), wantRefusalse: true},
		{name: "wrong repo rejected", env: nil, args: passwordPrompt("https://github.com/acme/other.git"), wantRefusalse: true},
		{name: "unquoted prompt rejected", env: nil, args: []string{"Password for https://github.com/acme/widgets.git:"}, wantRefusalse: true},
		{name: "unparseable quoted prompt rejected", env: nil, args: []string{"Password for 'not a url at all':"}, wantRefusalse: true},
		{name: "ssh remote rejected", env: nil, args: passwordPrompt("ssh://git@github.com/acme/widgets.git"), wantRefusalse: true},
		{name: "http remote rejected", env: nil, args: passwordPrompt("http://github.com/acme/widgets.git"), wantRefusalse: true},
		{name: "installation flag garbage", env: map[string]string{executor.EnvGitInstallation: "yes"}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "installation flag empty", env: map[string]string{executor.EnvGitInstallation: ""}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "missing owner", env: map[string]string{executor.EnvInstallationOwner: ""}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "missing endpoint", env: map[string]string{executor.EnvInstallationEndpoint: ""}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "missing namespace", env: map[string]string{"POD_NAMESPACE": ""}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "missing credentials secret", env: map[string]string{executor.EnvInstallationSecret: ""}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "missing repo identity", env: map[string]string{"COURIER_REPO": ""}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "missing repo url", env: map[string]string{"COURIER_REPO_URL": ""}, args: passwordPrompt(remote), wantRefusalse: true},
		{name: "missing api token file", env: map[string]string{executor.EnvAPITokenFile: ""}, args: passwordPrompt(remote), wantRefusalse: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request, ok := decideInstallationAskpass(installationTestEnv(testCase.env), testCase.args)
			if testCase.wantRefusalse {
				if ok {
					t.Fatalf("expected refusal, got %+v", request)
				}
				return
			}
			if !ok {
				t.Fatal("expected binding")
			}
			if request.identity != testCase.wantIdentity {
				t.Fatalf("identity = %q, want %q", request.identity, testCase.wantIdentity)
			}
			if request.serveUsername != testCase.wantUsername {
				t.Fatalf("serveUsername = %v, want %v", request.serveUsername, testCase.wantUsername)
			}
			if request.secretName != "run-7-provider-creds" {
				t.Fatalf("secret name = %q", request.secretName)
			}
			if request.namespace != "courier-system" || request.owner != "acme" || request.endpoint != "https://api.github.com" {
				t.Fatalf("request = %+v", request)
			}
		})
	}
}

// TestAskpassModeRouting pins the COURIER_ASKPASS routing: only the complete
// absence of COURIER_GIT_INSTALLATION selects the static path; any presence —
// empty or garbage included — selects the installation helper, which refuses
// closed and never emits a static credential.
func TestAskpassModeRouting(t *testing.T) {
	const staticToken = "static-pat-should-never-appear"
	for _, testCase := range []struct {
		name       string
		setFlag    bool
		flag       string
		wantStatic bool
	}{
		{name: "flag absent selects static askpass", wantStatic: true},
		{name: "flag present but empty selects installation refusal", setFlag: true},
		{name: "flag one selects installation helper", setFlag: true, flag: "1"},
		{name: "flag garbage selects installation refusal", setFlag: true, flag: "yes"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// App-mode configuration is deliberately incomplete beyond the
			// flag: the helper must refuse on its own, before any API call.
			env := map[string]string{
				"COURIER_GIT_USERNAME": "octocat",
				"COURIER_GIT_TOKEN":    staticToken,
			}
			if testCase.setFlag {
				env[executor.EnvGitInstallation] = testCase.flag
			}
			getenv := func(name string) string { return env[name] }
			lookupEnv := func(name string) (string, bool) { value, ok := env[name]; return value, ok }
			var stdout, stderr strings.Builder
			code := askpassMode(getenv, lookupEnv, passwordPrompt("https://github.com/acme/widgets.git"), &stdout, &stderr)
			if testCase.wantStatic {
				if code != 0 || stdout.String() != staticToken {
					t.Fatalf("static routing: code = %d stdout = %q, want the static token", code, stdout.String())
				}
				return
			}
			if code == exitSuccess || stdout.Len() != 0 {
				t.Fatalf("installation routing must refuse without serving a credential: code = %d stdout = %q", code, stdout.String())
			}
			if strings.Contains(stdout.String()+stderr.String(), staticToken) {
				t.Fatal("static credential leaked on an installation-mode prompt")
			}
			if !strings.Contains(stderr.String(), installationAskpassMessage) {
				t.Fatalf("stderr = %q, want the fixed refusal", stderr.String())
			}
		})
	}
}

func TestInstallationAskpassServesPinnedWorkAndBaseTokens(t *testing.T) {
	expires := time.Now().Add(time.Hour).UTC()
	document := installationTestDocument(t, "acme", "https://api.github.com",
		topology.InstallationTokenEntry{Repository: "acme/widgets", Token: "base-token-value", ExpiresAt: expires},
		topology.InstallationTokenEntry{Repository: "forks/widgets", Token: "work-token-value", ExpiresAt: expires},
	)
	reader := installationTestReader(t, document)
	env := installationTestEnv(map[string]string{
		"COURIER_HEAD_REPO":     "forks/widgets",
		"COURIER_BASE_REPO_URL": "https://github.com/acme/upstream.git",
		"COURIER_GIT_TOKEN":     "static-should-never-appear",
	})
	for _, testCase := range []struct {
		args   []string
		expect string
	}{
		{args: passwordPrompt("https://github.com/acme/widgets.git"), expect: "work-token-value"},
		{args: []string{"Username for 'https://github.com/acme/widgets.git':"}, expect: "x-access-token"},
		{args: passwordPrompt("https://github.com/acme/upstream.git"), expect: "base-token-value"},
		{args: []string{"Username for 'https://github.com/acme/upstream/':"}, expect: "x-access-token"},
	} {
		request, ok := decideInstallationAskpass(env, testCase.args)
		if !ok {
			t.Fatalf("prompt %v refused", testCase.args)
		}
		var stdout, stderr strings.Builder
		if code := serveInstallationAskpass(context.Background(), request, reader, &stdout, &stderr); code != 0 {
			t.Fatalf("prompt %v exit = %d stderr = %q", testCase.args, code, stderr.String())
		}
		if stdout.String() != testCase.expect {
			t.Fatalf("prompt %v stdout = %q, want %q", testCase.args, stdout.String(), testCase.expect)
		}
		if strings.Contains(stdout.String(), "static-should-never-appear") {
			t.Fatal("static token leaked")
		}
	}
}

func TestInstallationAskpassRefusals(t *testing.T) {
	valid := time.Now().Add(time.Hour).UTC()
	for _, testCase := range []struct {
		name     string
		document []byte
		entries  []topology.InstallationTokenEntry
		owner    string
		endpoint string
	}{
		{
			name:     "owner mismatch",
			document: installationTestDocument(t, "evil", "https://api.github.com", topology.InstallationTokenEntry{Repository: "acme/widgets", Token: "t", ExpiresAt: valid}),
		},
		{
			name:     "endpoint mismatch",
			document: installationTestDocument(t, "acme", "https://evil.example", topology.InstallationTokenEntry{Repository: "acme/widgets", Token: "t", ExpiresAt: valid}),
		},
		{
			name:     "unknown version",
			document: []byte(`{"version":99,"owner":"acme","endpoint":"https://api.github.com","entries":[{"repository":"acme/widgets","token":"t","expiresAt":"` + valid.Format(time.RFC3339) + `"}]}`),
		},
		{
			name:     "entry expiring inside the margin",
			document: installationTestDocument(t, "acme", "https://api.github.com", topology.InstallationTokenEntry{Repository: "acme/widgets", Token: "t", ExpiresAt: time.Now().Add(time.Minute)}),
		},
		{
			name:     "entry for another repository",
			document: installationTestDocument(t, "acme", "https://api.github.com", topology.InstallationTokenEntry{Repository: "acme/elsewhere", Token: "t", ExpiresAt: valid}),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			env := installationTestEnv(nil)
			args := passwordPrompt("https://github.com/acme/widgets.git")
			request, ok := decideInstallationAskpass(env, args)
			if !ok {
				t.Fatal("binding refused a valid prompt")
			}
			reader := installationTestReader(t, testCase.document)
			var stdout, stderr strings.Builder
			if code := serveInstallationAskpass(context.Background(), request, reader, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want refusal", code)
			}
			if stdout.Len() != 0 {
				t.Fatalf("refusal wrote to stdout: %q", stdout.String())
			}
			if stderr.String() != installationAskpassMessage+"\n" {
				t.Fatalf("stderr = %q", stderr.String())
			}
		})
	}
}

func TestInstallationAskpassUnpinnedIdentityNeverReads(t *testing.T) {
	request, ok := decideInstallationAskpass(installationTestEnv(nil), passwordPrompt("https://github.com/acme/widgets.git"))
	if !ok {
		t.Fatal("binding refused a valid prompt")
	}
	request.identity = "evil/repo"
	var stdout, stderr strings.Builder
	if code := serveInstallationAskpass(context.Background(), request, failReader{t: t}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want refusal", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("refusal wrote to stdout: %q", stdout.String())
	}
}

func TestInstallationAskpassMissingSecretRefuses(t *testing.T) {
	request, ok := decideInstallationAskpass(installationTestEnv(nil), passwordPrompt("https://github.com/acme/widgets.git"))
	if !ok {
		t.Fatal("binding refused a valid prompt")
	}
	reader := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	var stdout, stderr strings.Builder
	if code := serveInstallationAskpass(context.Background(), request, reader, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want refusal", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("refusal wrote to stdout: %q", stdout.String())
	}
}

func TestInstallationAskpassUnknownRemoteRefusesBeforeAPI(t *testing.T) {
	// No KUBERNETES_SERVICE_HOST/PORT exists in the test environment: an
	// unbound prompt must be refused by the pure binding alone, so the
	// runner never attempts an API reader.
	env := installationTestEnv(nil)
	var stdout, stderr strings.Builder
	if code := runInstallationAskpass(env, passwordPrompt("https://github.com/acme/other.git"), &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want refusal", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("refusal wrote to stdout: %q", stdout.String())
	}
}

// TestObserverGuardRemoteNeedsNoCredentialProvider pins the guard-routed
// observer contract: with no installation environment at all, the observer
// selects no credential provider and runs its git unauthenticated against the
// guard, which injects the credential upstream itself.
func TestObserverGuardRemoteNeedsNoCredentialProvider(t *testing.T) {
	provider, err := observerCredentials(func(name string) (string, bool) {
		return "", false
	}, func(string) string { return "" })
	if err != nil {
		t.Fatalf("observerCredentials: %v", err)
	}
	if provider != nil {
		t.Fatal("guard-routed observer unexpectedly selected credential provider")
	}
}

// TestObserverInstallationCredentialsAcceptsRenderedEnvironment pins the
// cross-package contract that once blocked App-mode observer pods, and that
// still serves pods launched before the guard pod: the environment
// internal/executor renders for a pre-#919 observer must be exactly
// sufficient for observerInstallationCredentials. If either side renames or
// drops a variable, this fails instead of every such pod failing at launch.
func TestObserverInstallationCredentialsAcceptsRenderedEnvironment(t *testing.T) {
	env := map[string]string{
		executor.EnvGitInstallation:      "1",
		executor.EnvInstallationOwner:    "acme",
		executor.EnvInstallationEndpoint: "https://api.github.invalid",
		executor.EnvInstallationSecret:   "run-7-provider-creds",
		"POD_NAMESPACE":                  "courier-system",
		"COURIER_REPO":                   "acme/widgets",
		"COURIER_HEAD_REPO":              "fork/widgets",
		"COURIER_REPO_URL":               "https://github.invalid/fork/widgets.git",
		"COURIER_BASE_REPO_URL":          "https://github.invalid/acme/widgets.git",
		"KUBERNETES_SERVICE_HOST":        "127.0.0.1",
		"KUBERNETES_SERVICE_PORT":        "6443",
	}
	provider, err := observerInstallationCredentials(func(name string) string { return env[name] })
	if err == nil {
		// Construction normally proceeds; a test host with a real projected
		// token still must not resolve un-pinned remotes.
		if _, _, err := provider(context.Background(), "https://github.invalid/other/repo.git"); err == nil {
			t.Fatal("provider must refuse an un-pinned remote")
		}
		return
	}
	// The blocker this test pins was a BINDING refusal ("remote binding is
	// incomplete"/"configuration is incomplete"): every variable the
	// internal/executor observer rendering provides must satisfy the
	// provider up to the API reader itself. Reaching the reader stage with a
	// token file this host does not have is the expected pass; any earlier
	// named refusal means the rendered environment drifted.
	if err.Error() != "observer: installation git API reader unavailable" {
		t.Fatalf("rendered environment failed before the API reader: %v", err)
	}
}
