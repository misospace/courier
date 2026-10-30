package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/forge"
	couriergithub "github.com/misospace/courier/internal/github"
	"github.com/misospace/courier/internal/source/cron"
	"github.com/misospace/courier/internal/source/dispatch"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
)

type observerWithHeadResolver struct{}

func (observerWithHeadResolver) Observe(context.Context, string, controller.HeadRef) (controller.PRObservation, error) {
	return controller.PRObservation{}, nil
}

func (observerWithHeadResolver) ResolveHead(context.Context, *courierv1alpha1.CoderRun) (controller.HeadRef, error) {
	return controller.HeadRef{Repo: "acme/demo", Branch: "feature/existing"}, nil
}

func TestDispatchPRStateCheckerRequiresGitHubObserver(t *testing.T) {
	if _, err := dispatchPRStateChecker(nil); !errors.Is(err, dispatch.ErrPRStateCheckerNeeded) {
		t.Fatalf("dispatchPRStateChecker() error = %v, want checker-required", err)
	}
}

func TestExistingPRHeadResolver(t *testing.T) {
	observer := observerWithHeadResolver{}
	if got := existingPRHeadResolver(observer); got == nil {
		t.Fatal("existingPRHeadResolver() returned nil for a compatible observer")
	}
	if got := existingPRHeadResolver(nil); got != nil {
		t.Fatal("existingPRHeadResolver(nil) returned a resolver")
	}
}

func TestPullRequestReaderWiresForgeCapability(t *testing.T) {
	if got := pullRequestReader(nil); got != nil {
		t.Fatal("pullRequestReader(nil) returned a reader")
	}
	if got := pullRequestReader(observerWithHeadResolver{}); got != nil {
		t.Fatal("pullRequestReader() returned a reader for a non-GitHub observer")
	}
	client, err := couriergithub.New("test-token")
	if err != nil {
		t.Fatalf("build GitHub client: %v", err)
	}
	reader := pullRequestReader(couriergithub.Observer{Client: client})
	if reader == nil {
		t.Fatal("pullRequestReader() returned nil for the GitHub observer")
	}
	if availability := reader.Capabilities().Available(forge.CapabilityReadPullRequest); !availability.Available {
		t.Fatalf("read-pull-request capability = %#v, want available for the wired reader", availability)
	}
}

func TestResolveDispatchBindings(t *testing.T) {
	tests := []struct {
		name      string
		queueLane string
		lane      string
		repeated  []string
		want      []dispatchBinding
		wantErr   bool
	}{
		{
			name:      "shorthand only",
			queueLane: "queue-a",
			lane:      "lane-a",
			want:      []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a", agentName: "courier"}},
		},
		{
			name:     "single repeated binding preserves bare identity",
			repeated: []string{"queue-a:lane-a"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a", agentName: "courier"}},
		},
		{
			name:     "repeated only",
			repeated: []string{"queue-a:lane-a", "queue-b:lane-b"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a", agentName: "courier-queue-a", sourceAgent: "courier-queue-a"}, {queueLane: "queue-b", laneProfile: "lane-b", agentName: "courier-queue-b", sourceAgent: "courier-queue-b"}},
		},
		{
			name:      "shorthand and repeated mix rejected",
			queueLane: "queue-a",
			lane:      "lane-a",
			repeated:  []string{"queue-b:lane-b"},
			wantErr:   true,
		},
		{
			name:      "queue lane only rejected",
			queueLane: "queue-a",
			wantErr:   true,
		},
		{
			name:    "lane profile only rejected",
			lane:    "lane-a",
			wantErr: true,
		},
		{
			name:     "repeated without colon rejected",
			repeated: []string{"queue-a"},
			wantErr:  true,
		},
		{
			name:     "repeated empty half rejected",
			repeated: []string{"queue-a:"},
			wantErr:  true,
		},
		{
			name:     "duplicate queue with explicit agents accepted",
			repeated: []string{"queue-a:lane-a:courier-local", "queue-a:lane-b:courier-cloud"},
			want: []dispatchBinding{
				{queueLane: "queue-a", laneProfile: "lane-a", agentName: "courier-local", sourceAgent: "courier-local"},
				{queueLane: "queue-a", laneProfile: "lane-b", agentName: "courier-cloud", sourceAgent: "courier-cloud"},
			},
		},
		{
			name:     "duplicate identity rejected",
			repeated: []string{"queue-a:lane-a:worker", "queue-b:lane-b:worker"},
			wantErr:  true,
		},
		{
			name:     "duplicate lane profile accepted",
			repeated: []string{"queue-a:lane-x", "queue-b:lane-x"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-x", agentName: "courier-queue-a", sourceAgent: "courier-queue-a"}, {queueLane: "queue-b", laneProfile: "lane-x", agentName: "courier-queue-b", sourceAgent: "courier-queue-b"}},
		},
		{
			name:     "two shared queue bindings and explicit third identity",
			repeated: []string{"local:local:courier-local", "local:minimax:courier-cloud", "escalated:frontier:courier-frontier"},
			want: []dispatchBinding{
				{queueLane: "local", laneProfile: "local", agentName: "courier-local", sourceAgent: "courier-local"},
				{queueLane: "local", laneProfile: "minimax", agentName: "courier-cloud", sourceAgent: "courier-cloud"},
				{queueLane: "escalated", laneProfile: "frontier", agentName: "courier-frontier", sourceAgent: "courier-frontier"},
			},
		},
		{
			// Multi-binding derivation preserves the established distinct identity
			// default; explicit third segments can instead share a queue safely.
			name:     "multi-binding derives per-binding agent name",
			repeated: []string{"local:local", "minimax:minimax", "escalated:frontier"},
			want: []dispatchBinding{
				{queueLane: "local", laneProfile: "local", agentName: "courier-local", sourceAgent: "courier-local"},
				{queueLane: "minimax", laneProfile: "minimax", agentName: "courier-minimax", sourceAgent: "courier-minimax"},
				{queueLane: "escalated", laneProfile: "frontier", agentName: "courier-escalated", sourceAgent: "courier-escalated"},
			},
		},
		{
			name:     "whitespace trimmed",
			repeated: []string{" queue-a : lane-a "},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a", agentName: "courier"}},
		},
		{
			name:     "repeated whitespace-only rejected",
			repeated: []string{" "},
			wantErr:  true,
		},
		{
			name:     "extra colon in lane profile rejected",
			repeated: []string{"queue-a:lane:a:b"},
			wantErr:  true,
		},
		{
			name:      "whitespace-only shorthand rejected",
			queueLane: " ",
			wantErr:   true,
		},
		{
			name: "all empty",
			want: []dispatchBinding{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveDispatchBindings("courier", test.queueLane, test.lane, test.repeated)
			if test.wantErr {
				if err == nil {
					t.Fatalf("resolveDispatchBindings() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDispatchBindings() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("resolveDispatchBindings() = %#v, want %#v", got, test.want)
			}
		})
	}
}

// TestResolveDispatchBindingsBoundsTheBareConnectorIdentity pins the bound on
// the one connector identity Dispatch adapters share on purpose. The manager
// registers a legacy bare adapter for retained runs, and a binding with no
// source agent registers under the same bare "dispatch" identity because both
// talk to the same Dispatch agent. That sharing is only sound while at most
// one binding carries the bare identity: two adapters under one identity
// would merge independent connectors' series.
//
// The table is a characterisation of today's binding derivation, read
// through the invariant: the count of bindings that carry no source agent.
func TestResolveDispatchBindingsBoundsTheBareConnectorIdentity(t *testing.T) {
	const baseAgent = "courier"
	tests := []struct {
		name      string
		queueLane string
		lane      string
		repeated  []string
		wantBare  int
	}{
		{
			name:      "shorthand keeps the bare identity",
			queueLane: "queue-a",
			lane:      "lane-a",
			wantBare:  1,
		},
		{
			name:     "single repeated binding keeps the bare identity",
			repeated: []string{"queue-a:lane-a"},
			wantBare: 1,
		},
		{
			name:     "repeated bindings derive one identity each",
			repeated: []string{"queue-a:lane-a", "queue-b:lane-b"},
			wantBare: 0,
		},
		{
			name:     "explicit agent identities keep the bare identity free",
			repeated: []string{"queue-a:lane-a:agent-a", "queue-b:lane-b:agent-b"},
			wantBare: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bindings, err := resolveDispatchBindings(baseAgent, test.queueLane, test.lane, test.repeated)
			if err != nil {
				t.Fatalf("resolveDispatchBindings() error = %v", err)
			}
			bare := 0
			for _, binding := range bindings {
				if binding.sourceAgent != "" {
					continue
				}
				bare++
				// The bare identity is one Dispatch agent's identity, so the
				// binding must use the same agent name the legacy client uses.
				if binding.agentName != baseAgent {
					t.Fatalf("bare binding agent name = %q, want the base agent name %q", binding.agentName, baseAgent)
				}
			}
			if bare != test.wantBare {
				t.Fatalf("bindings sharing the bare identity = %d, want %d", bare, test.wantBare)
			}
			if bare > 1 {
				t.Fatalf("%d bindings share the bare connector identity, want at most 1", bare)
			}
		})
	}
}

func TestBuildSecureControlRejectsTaintedLegacyCredential(t *testing.T) {
	registryFile := filepath.Join(t.TempDir(), "providers.json")
	providers := `{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"courier-github","key":"token"}},"serves":["*"]}]}`
	if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := buildSecureControl(secureConfig{
		providersFile:   registryFile,
		runNamespace:    "runs",
		harnessImage:    "harness:test",
		probeImage:      "busybox:test",
		legacyGitSecret: "courier-github",
		observer:        observerWithHeadResolver{},
		operatorNS:      "courier-system",
	})
	if err == nil || !strings.Contains(err.Error(), "tainted") {
		t.Fatalf("a registration referencing a legacy-exposed Secret must fail startup, got %v", err)
	}
}

func TestBuildSecureControlRequiresObserverCredential(t *testing.T) {
	registryFile := filepath.Join(t.TempDir(), "providers.json")
	providers := `{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["*"]}]}`
	if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildSecureControl(secureConfig{
		providersFile: registryFile,
		runNamespace:  "runs",
	}); err == nil || !strings.Contains(err.Error(), "observer") {
		t.Fatalf("secure mode without an admission-read identity must fail startup, got %v", err)
	}
}

// recordingForgeTransport records every request the GitHub client makes and
// answers repository reads, so wiring tests can assert the destination host
// and path without any network.
type recordingForgeTransport struct {
	requests []*http.Request
}

func (t *recordingForgeTransport) Do(req *http.Request) (*http.Response, error) {
	t.requests = append(t.requests, req.Clone(context.Background()))
	body := io.NopCloser(strings.NewReader(`{"id":7,"full_name":"acme/widgets","default_branch":"main"}`))
	return &http.Response{
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       body,
	}, nil
}

func TestSecureAdmissionProviderHonorsRegistrationEndpoint(t *testing.T) {
	const operatorToken = "operator-read-token"
	tests := []struct {
		name     string
		endpoint string
		wantHost string
		wantPath string
	}{
		{
			name:     "enterprise registration keeps reads on its own host and API prefix",
			endpoint: "https://ghes.example.test/api/v3",
			wantHost: "ghes.example.test",
			wantPath: "/api/v3/repos/acme/widgets",
		},
		{
			name:     "enterprise API prefix keeps its path with a trailing slash",
			endpoint: "https://ghes.example.test/api/v3/",
			wantHost: "ghes.example.test",
			wantPath: "/api/v3/repos/acme/widgets",
		},
		{
			name:     "github.com registration keeps the default destination",
			endpoint: "https://api.github.com/",
			wantHost: "api.github.com",
			wantPath: "/repos/acme/widgets",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registryFile := filepath.Join(t.TempDir(), "providers.json")
			providers := fmt.Sprintf(`{"providers":[{"name":"primary","type":"github","endpoint":%q,"credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["*"]}]}`, test.endpoint)
			if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
				t.Fatal(err)
			}
			transport := &recordingForgeTransport{}
			observerClient, err := couriergithub.NewClient("https://unexpected.invalid/", operatorToken, transport)
			if err != nil {
				t.Fatal(err)
			}
			control, err := buildSecureControl(secureConfig{
				providersFile: registryFile,
				runNamespace:  "runs",
				harnessImage:  "harness:test",
				probeImage:    "busybox:test",
				observer:      couriergithub.Observer{Client: observerClient},
				operatorNS:    "courier-system",
				restConfig:    &rest.Config{Host: "https://127.0.0.1:1"},
			})
			if err != nil {
				t.Fatalf("buildSecureControl: %v", err)
			}
			registration, err := control.Config.Registry.Select("acme/widgets")
			if err != nil {
				t.Fatalf("select registration: %v", err)
			}
			admission, err := control.Config.Providers(context.Background(), registration)
			if err != nil {
				t.Fatalf("providers factory: %v", err)
			}
			if got := admission.Provider.Config().Endpoint; got != test.endpoint {
				t.Fatalf("provider endpoint = %q, want %q", got, test.endpoint)
			}
			repo, err := admission.Policy.ResolveRepository(context.Background(), "acme/widgets")
			if err != nil || repo.Canonical != "acme/widgets" {
				t.Fatalf("resolve repository = %#v, %v", repo, err)
			}
			if len(transport.requests) != 1 {
				t.Fatalf("recorded %d requests, want exactly one", len(transport.requests))
			}
			req := transport.requests[0]
			if req.URL.Host == "api.github.com" && test.wantHost != "api.github.com" {
				t.Fatalf("a custom registration must not reach the public API: host = %q", req.URL.Host)
			}
			if req.URL.Host != test.wantHost {
				t.Fatalf("request host = %q, want %q", req.URL.Host, test.wantHost)
			}
			if req.URL.Path != test.wantPath {
				t.Fatalf("request path = %q, want %q", req.URL.Path, test.wantPath)
			}
			if got := req.Header.Get("Authorization"); got != "Bearer "+operatorToken {
				t.Fatalf("authorization = %q, want the operator read identity carried over", got)
			}
		})
	}
}

// observationRecordingTransport answers the operator's observation reads
// (head listings, exact pull-request reads, check runs, commit statuses) and
// records every request destination, so wiring tests can assert which host
// each read reached without any network.
type observationRecordingTransport struct {
	requests []*http.Request
}

func (t *observationRecordingTransport) Do(req *http.Request) (*http.Response, error) {
	t.requests = append(t.requests, req.Clone(context.Background()))
	var body string
	switch path := req.URL.Path; {
	case strings.HasSuffix(path, "/pulls"):
		body = `[{"number":7,"state":"open","head":{"ref":"agent/work","sha":"headsha","repo":{"full_name":"acme/widgets"}},"base":{"ref":"main","repo":{"full_name":"acme/widgets"}}}]`
	case strings.HasSuffix(path, "/pulls/7"):
		body = `{"number":7,"state":"open","head":{"ref":"agent/work","sha":"headsha","repo":{"full_name":"acme/widgets"}},"base":{"ref":"main","repo":{"full_name":"acme/widgets"}}}`
	case strings.HasSuffix(path, "/check-runs"):
		body = `{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"success"}]}`
	case strings.HasSuffix(path, "/status"):
		body = `{"total_count":0,"statuses":[]}`
	default:
		body = `{}`
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

// TestObserverReadsRouteThroughRegistrationEndpoint is the #377 wiring test:
// every operator-side forge read — post-review CI observation, existing-PR
// head resolution, post-review merge observation, and the Dispatch PR-state
// check — must go to the host of the registration serving the repository, and
// a deployment with no registry must stay on the GitHub.com default.
func TestObserverReadsRouteThroughRegistrationEndpoint(t *testing.T) {
	const operatorToken = "operator-read-token"
	tests := []struct {
		name        string
		useRegistry bool
		wantHost    string
		wantPrefix  string
	}{
		{
			name:        "enterprise registration keeps observation reads on its own host",
			useRegistry: true,
			wantHost:    "ghes.example.test",
			wantPrefix:  "/api/v3/repos/acme/widgets",
		},
		{
			name:        "no registry keeps the GitHub.com default destination",
			useRegistry: false,
			wantHost:    "api.github.com",
			wantPrefix:  "/repos/acme/widgets",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var registry *forge.Registry
			if test.useRegistry {
				registryFile := filepath.Join(t.TempDir(), "providers.json")
				providers := `{"providers":[{"name":"ghe","type":"github","endpoint":"https://ghes.example.test/api/v3","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["acme/*"]}]}`
				if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
					t.Fatal(err)
				}
				loaded, err := loadForgeRegistry(registryFile)
				if err != nil {
					t.Fatalf("loadForgeRegistry: %v", err)
				}
				registry = loaded
			}
			transport := &observationRecordingTransport{}
			client, err := couriergithub.New(operatorToken, transport)
			if err != nil {
				t.Fatal(err)
			}
			observer := couriergithub.Observer{Client: client, Registrations: registry}
			ctx := context.Background()

			observation, err := observer.Observe(ctx, "acme/widgets", controller.HeadRef{Branch: "agent/work"})
			if err != nil || observation.PR != "7" || len(observation.Checks) != 1 {
				t.Fatalf("Observe() = %#v, %v", observation, err)
			}
			resolver := existingPRHeadResolver(observer)
			if resolver == nil {
				t.Fatal("existingPRHeadResolver() returned nil for the GitHub observer")
			}
			head, err := resolver.ResolveHead(ctx, &courierv1alpha1.CoderRun{Spec: courierv1alpha1.CoderRunSpec{Repo: "acme/widgets", Ref: 7}})
			if err != nil || head.Branch != "agent/work" {
				t.Fatalf("ResolveHead() = %#v, %v", head, err)
			}
			reader := pullRequestReader(observer)
			if reader == nil {
				t.Fatal("pullRequestReader() returned nil for the GitHub observer")
			}
			pull, err := reader.ReadPullRequest(ctx, forge.PullRequestRef{Repo: "acme/widgets", Number: 7})
			if err != nil || pull.Number != 7 {
				t.Fatalf("ReadPullRequest() = %#v, %v", pull, err)
			}
			checker, err := dispatchPRStateChecker(observer)
			if err != nil {
				t.Fatalf("dispatchPRStateChecker: %v", err)
			}
			state, err := checker.CheckPullRequestState(ctx, "acme/widgets", 7)
			if err != nil || state.State != "open" {
				t.Fatalf("Dispatch PR-state check = %#v, %v", state, err)
			}

			if len(transport.requests) == 0 {
				t.Fatal("no observation requests recorded")
			}
			for _, req := range transport.requests {
				if req.URL.Host != test.wantHost {
					t.Fatalf("observation read for %s went to %q, want %q", req.URL.Path, req.URL.Host, test.wantHost)
				}
				if !strings.HasPrefix(req.URL.Path, test.wantPrefix) {
					t.Fatalf("observation read path = %q, want prefix %q", req.URL.Path, test.wantPrefix)
				}
				if got := req.Header.Get("Authorization"); got != "Bearer "+operatorToken {
					t.Fatalf("authorization = %q, want the operator read identity carried over", got)
				}
			}
		})
	}
}

// TestGithubObserverCarriesRegistry pins the production constructor handoff:
// the loaded registry rides onto the observer that every observation read
// routes through, so wiring the registry is not merely a parameter that the
// adapters happen to bypass.
func TestGithubObserverCarriesRegistry(t *testing.T) {
	registryFile := filepath.Join(t.TempDir(), "providers.json")
	providers := `{"providers":[{"name":"ghe","type":"github","endpoint":"https://ghes.example.test/api/v3","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["acme/*"]}]}`
	if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	registry, err := loadForgeRegistry(registryFile)
	if err != nil {
		t.Fatalf("loadForgeRegistry: %v", err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "courier-github", Namespace: "courier-system"},
		Data:       map[string][]byte{"token": []byte("operator-read-token")},
	}).Build()

	observer, err := githubObserver(context.Background(), reader, "courier-system", "courier-github", "", "", "", registry)
	if err != nil {
		t.Fatalf("githubObserver() error = %v", err)
	}
	got, ok := observer.(couriergithub.Observer)
	if !ok {
		t.Fatalf("githubObserver() returned %T, want couriergithub.Observer", observer)
	}
	if got.Registrations != registry {
		t.Fatal("githubObserver() did not carry the loaded registry onto the observer")
	}
	if _, err := got.ClientForRepository("acme/widgets"); err != nil {
		t.Fatalf("ClientForRepository() error = %v, want the observer to route through the registry", err)
	}
}

// TestObserverRegistryWithoutMatchFailsClosed pins that a loaded registry which
// cannot settle a repository is a read error and never a silent send to the
// observer client's default destination.
func TestObserverRegistryWithoutMatchFailsClosed(t *testing.T) {
	registryFile := filepath.Join(t.TempDir(), "providers.json")
	providers := `{"providers":[{"name":"ghe","type":"github","endpoint":"https://ghes.example.test/api/v3","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["acme/*"]}]}`
	if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	registry, err := loadForgeRegistry(registryFile)
	if err != nil {
		t.Fatalf("loadForgeRegistry: %v", err)
	}
	transport := &observationRecordingTransport{}
	client, err := couriergithub.New("operator-read-token", transport)
	if err != nil {
		t.Fatal(err)
	}
	observer := couriergithub.Observer{Client: client, Registrations: registry}
	ctx := context.Background()

	if _, err := observer.Observe(ctx, "other/widgets", controller.HeadRef{Branch: "agent/work"}); err == nil || !strings.Contains(err.Error(), "no provider registration serves") {
		t.Fatalf("Observe() error = %v, want the fail-closed selection error", err)
	}
	if _, err := pullRequestReader(observer).ReadPullRequest(ctx, forge.PullRequestRef{Repo: "other/widgets", Number: 7}); err == nil || !strings.Contains(err.Error(), "no provider registration serves") {
		t.Fatalf("ReadPullRequest() error = %v, want the fail-closed selection error", err)
	}
	checker, err := dispatchPRStateChecker(observer)
	if err != nil {
		t.Fatalf("dispatchPRStateChecker: %v", err)
	}
	if _, err := checker.CheckPullRequestState(ctx, "other/widgets", 7); err == nil || !strings.Contains(err.Error(), "no provider registration serves") {
		t.Fatalf("Dispatch PR-state check error = %v, want the fail-closed selection error", err)
	}
	if len(transport.requests) != 0 {
		t.Fatalf("an unsettled repository made %d requests; it must make none, not fall back to the default host", len(transport.requests))
	}
}

// TestDefaultProbeImageMatchesChartValue keeps the manager's built-in probe
// image and the chart's secure.probeImage in lockstep. A deployment through
// the chart and a bare binary with defaults must probe with the same image,
// and that image must be pinned by digest so a republished tag cannot change
// the code that decides whether a node passes preflight. The digest is what
// pins the image; the tag beside it is documentation of which release that
// digest belongs to.
func TestDefaultProbeImageMatchesChartValue(t *testing.T) {
	pinned := regexp.MustCompile(`^[^@\s]+@sha256:[0-9a-f]{64}$`)
	if !pinned.MatchString(defaultsProbeImage) {
		t.Fatalf("defaultsProbeImage = %q, want a digest-pinned image reference", defaultsProbeImage)
	}

	data, err := os.ReadFile(filepath.Join("..", "charts", "courier", "values.yaml"))
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	var values struct {
		Secure struct {
			ProbeImage string `json:"probeImage"`
		} `json:"secure"`
	}
	if err := yaml.Unmarshal(data, &values); err != nil {
		t.Fatalf("parse chart values: %v", err)
	}
	chartImage := strings.TrimSpace(values.Secure.ProbeImage)
	if !pinned.MatchString(chartImage) {
		t.Fatalf("chart secure.probeImage = %q, want a digest-pinned image reference", chartImage)
	}
	if chartImage != defaultsProbeImage {
		t.Fatalf("chart secure.probeImage = %q, manager default = %q; they must name the same pinned image", chartImage, defaultsProbeImage)
	}
}

func TestValidateEvidenceFlags(t *testing.T) {
	tests := []struct {
		name      string
		keySecret string
		service   string
		bind      string
		wantErr   string // substring required in the error; empty means no error
	}{
		{
			name: "all empty disables wiring",
		},
		{
			name:      "key secret and service pair",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
		},
		{
			name:      "key secret, service, and bind",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
			bind:      "0.0.0.0:8443",
		},
		{
			name:      "key secret without service",
			keySecret: "courier-evidence-key",
			wantErr:   "both --evidence-intake-key-secret and --evidence-intake-service",
		},
		{
			name:    "service without key secret",
			service: "http://evidence-intake:8443",
			wantErr: "both --evidence-intake-key-secret and --evidence-intake-service",
		},
		{
			name:    "bind without key secret",
			bind:    "0.0.0.0:8443",
			wantErr: "--evidence-intake-bind requires --evidence-intake-key-secret",
		},
		{
			name:      "whitespace-only bind ignored",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
			bind:      "   ",
		},
		{
			// Settled decision: key+service without bind stays legal so
			// deployments that predate --evidence-intake-bind keep starting.
			// main() logs that capture POSTs will be refused.
			name:      "key secret and service without bind stays valid",
			keySecret: "courier-evidence-key",
			service:   "http://evidence-intake:8443",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateEvidenceFlags(test.keySecret, test.service, test.bind)
			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("validateEvidenceFlags() error = nil, want error mentioning %q", test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("validateEvidenceFlags() error = %q, want it to mention %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateEvidenceFlags() error = %v, want none", err)
			}
		})
	}
}

// stalledReader mimics an API server that accepts the connection but never
// answers: every Get blocks until the caller's context ends.
type stalledReader struct{ client.Reader }

func (stalledReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestStartupReadsSurfaceDeadlineErrors(t *testing.T) {
	reader := stalledReader{}

	t.Run("evidence intake key read", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err := loadEvidenceKey(ctx, reader, "courier-system", "courier-evidence-key")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("loadEvidenceKey() error = %v, want a deadline-exceeded error", err)
		}
		if !strings.Contains(err.Error(), "read evidence intake key Secret courier-system/courier-evidence-key") {
			t.Fatalf("loadEvidenceKey() error = %q, want it to name the failing read", err)
		}
	})

	t.Run("GitHub observer read", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err := githubObserver(ctx, reader, "courier-system", "courier-github", "", "", "", nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("githubObserver() error = %v, want a deadline-exceeded error", err)
		}
		if !strings.Contains(err.Error(), "read GitHub credential Secret courier-system/courier-github") {
			t.Fatalf("githubObserver() error = %q, want it to name the failing read", err)
		}
	})
}

func TestLoadEvidenceKey(t *testing.T) {
	const keyValue = "a-test-hmac-key-of-some-length"

	tests := []struct {
		name       string
		namespace  string
		secretName string
		data       map[string][]byte // nil: no Secret in the namespace
		want       []byte
		wantErr    string // substring required in the error
	}{
		{
			name:       "happy path",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"key": []byte(keyValue)},
			want:       []byte(keyValue),
		},
		{
			name:       "secret missing",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			wantErr:    "not found",
		},
		{
			name:       "no key entry",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"other": []byte("not-the-key")},
			wantErr:    `no non-empty "key" entry`,
		},
		{
			name:       "whitespace-only key",
			namespace:  "courier-system",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"key": []byte("   ")},
			wantErr:    `no non-empty "key" entry`,
		},
		{
			name:       "empty namespace",
			namespace:  "",
			secretName: "courier-evidence-key",
			data:       map[string][]byte{"key": []byte(keyValue)},
			wantErr:    "POD_NAMESPACE",
		},
		{
			name:       "empty name",
			namespace:  "courier-system",
			secretName: "",
			data:       map[string][]byte{"key": []byte(keyValue)},
			wantErr:    "name is required",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("add core scheme: %v", err)
			}
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if test.data != nil {
				builder = builder.WithRuntimeObjects(&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "courier-evidence-key",
						Namespace: "courier-system",
					},
					Data: test.data,
				})
			}
			fakeClient := builder.Build()

			got, err := loadEvidenceKey(context.Background(), fakeClient, test.namespace, test.secretName)
			if test.wantErr != "" {
				if err == nil {
					t.Fatalf("loadEvidenceKey() error = nil, want error mentioning %q", test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("loadEvidenceKey() error = %q, want it to mention %q", err, test.wantErr)
				}
				if strings.Contains(err.Error(), keyValue) {
					t.Fatalf("loadEvidenceKey() error = %q must not contain the key value", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadEvidenceKey() error = %v", err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("loadEvidenceKey() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestCronLaneGroups(t *testing.T) {
	schedules := []cron.Schedule{
		{Name: "a", Lane: "frontier"},
		{Name: "b", Lane: "local"},
		{Name: "c", Lane: "frontier"},
	}
	groups := cronLaneGroups(schedules)
	if len(groups) != 2 {
		t.Fatalf("len(groups) = %d, want 2", len(groups))
	}
	if groups[0].lane != "frontier" || len(groups[0].schedules) != 2 ||
		groups[0].schedules[0].Name != "a" || groups[0].schedules[1].Name != "c" {
		t.Fatalf("frontier group = %+v", groups[0])
	}
	if groups[1].lane != "local" || len(groups[1].schedules) != 1 || groups[1].schedules[0].Name != "b" {
		t.Fatalf("local group = %+v", groups[1])
	}
	if groups := cronLaneGroups(nil); len(groups) != 0 {
		t.Fatalf("empty input produced %+v", groups)
	}
}

func TestResolveOwnership(t *testing.T) {
	tests := []struct {
		name        string
		input       ownershipInput
		wantOwned   []string
		wantWatched []string
		wantErr     string
	}{
		{
			name:  "legacy with no list keeps the all-namespaces default",
			input: ownershipInput{podNamespace: "courier-system"},
		},
		{
			name:  "explicit list is authoritative and adds the operator namespace to the watch set",
			input: ownershipInput{owned: []string{"runs-a", "runs-b"}, podNamespace: "courier-system"},
			wantOwned: []string{
				"runs-a", "runs-b",
			},
			wantWatched: []string{"runs-a", "runs-b", "courier-system"},
		},
		{
			name:  "duplicate and whitespace entries collapse",
			input: ownershipInput{owned: []string{" runs-a ", "runs-a", "runs-a"}},
			wantOwned: []string{
				"runs-a",
			},
			wantWatched: []string{"runs-a"},
		},
		{
			name:    "empty and invalid names fail closed",
			input:   ownershipInput{owned: []string{"Bad_Namespace"}},
			wantErr: "not a valid namespace name",
		},
		{
			name:    "blank entries fail closed",
			input:   ownershipInput{owned: []string{"runs-a", " "}},
			wantErr: "not a valid namespace name",
		},
		{
			name:  "secure without a list owns exactly its run namespace",
			input: ownershipInput{secureMode: true, runNamespace: "runs-a", podNamespace: "runs-a"},
			wantOwned: []string{
				"runs-a",
			},
			wantWatched: []string{"runs-a"},
		},
		{
			name:    "secure requires its run namespace",
			input:   ownershipInput{secureMode: true},
			wantErr: "requires --run-namespace",
		},
		{
			name:    "secure rejects a boundary that excludes its run namespace",
			input:   ownershipInput{secureMode: true, runNamespace: "runs-a", owned: []string{"runs-b"}},
			wantErr: "must be among the --owned-namespace values",
		},
		{
			name:  "secure accepts a boundary carrying its run namespace",
			input: ownershipInput{secureMode: true, runNamespace: "runs-a", owned: []string{"runs-a", "runs-b"}, podNamespace: "courier-system"},
			wantOwned: []string{
				"runs-a", "runs-b",
			},
			wantWatched: []string{"runs-a", "runs-b", "courier-system"},
		},
		{
			name:  "discovery is allowed where the operator namespace is owned",
			input: ownershipInput{owned: []string{"runs-a"}, podNamespace: "runs-a", discoveryEnabled: true},
			wantOwned: []string{
				"runs-a",
			},
			wantWatched: []string{"runs-a"},
		},
		{
			name:    "discovery fails closed when the operator namespace is foreign",
			input:   ownershipInput{owned: []string{"runs-a"}, podNamespace: "courier-system", discoveryEnabled: true},
			wantErr: "materialize CoderRuns in the operator namespace",
		},
		{
			name:    "secure discovery fails closed when the operator runs outside the run namespace",
			input:   ownershipInput{secureMode: true, runNamespace: "runs-a", podNamespace: "courier-system", discoveryEnabled: true},
			wantErr: "materialize CoderRuns in the operator namespace",
		},
		{
			name:  "an unbounded legacy deployment discovers freely",
			input: ownershipInput{podNamespace: "courier-system", discoveryEnabled: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owned, watched, err := resolveOwnership(test.input)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want one containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveOwnership: %v", err)
			}
			if !reflect.DeepEqual(owned, test.wantOwned) {
				t.Fatalf("owned = %q, want %q", owned, test.wantOwned)
			}
			if !reflect.DeepEqual(watched, test.wantWatched) {
				t.Fatalf("watched = %q, want %q", watched, test.wantWatched)
			}
		})
	}
}

func TestControllerCacheOptionsScopeRunKinds(t *testing.T) {
	if opts := controllerCacheOptions(nil); len(opts.ByObject) != 0 {
		t.Fatalf("empty watch set restricted %d object kinds, want the unrestricted default", len(opts.ByObject))
	}
	opts := controllerCacheOptions([]string{"runs-a", "courier-system"})
	want := map[string]bool{
		"CoderRun":    false,
		"LaneProfile": false,
		"Pod":         false,
	}
	for object, scoped := range opts.ByObject {
		kind := reflect.TypeOf(object).Elem().Name()
		if _, ok := want[kind]; !ok {
			t.Fatalf("cache restricted unexpected kind %s; preflight-visible kinds must stay cluster-wide", kind)
		}
		want[kind] = true
		if len(scoped.Namespaces) != 2 {
			t.Fatalf("kind %s watches %d namespaces, want the watched set", kind, len(scoped.Namespaces))
		}
		for _, namespace := range []string{"runs-a", "courier-system"} {
			if _, ok := scoped.Namespaces[namespace]; !ok {
				t.Fatalf("kind %s does not watch %q", kind, namespace)
			}
		}
	}
	for kind, seen := range want {
		if !seen {
			t.Fatalf("cache did not restrict %s", kind)
		}
	}
}

// TestUncachedReadObjectsCoverNoWatchKinds guards the invariant that aligns
// the manager client's reads with the shipped chart RBAC (#641): every kind
// read by-name during secure admission, preflight or provisioning must be
// excluded from the informer cache (the chart grants those kinds get/list but
// not watch), while the three kinds the chart does grant watch on must stay
// cached and out of the uncached set.
func TestUncachedReadObjectsCoverNoWatchKinds(t *testing.T) {
	// Kinds the chart grants get/list but never watch, all point-read by the
	// secure path. A read of any of these through the cache starts a forbidden
	// cluster-wide informer and wedges, so each must appear here.
	wantUncached := map[string]bool{
		"Secret":         false,
		"Service":        false,
		"ServiceAccount": false,
		"Namespace":      false,
		"Node":           false,
		"Role":           false,
	}
	// Kinds the chart grants watch on and the cache legitimately holds. They
	// must never be forced uncached: doing so would drop the watch-driven
	// reconciliation that wake-on-event correctness depends on.
	watched := map[string]bool{"CoderRun": true, "LaneProfile": true, "Pod": true}
	for _, object := range uncachedReadObjects() {
		kind := reflect.TypeOf(object).Elem().Name()
		if watched[kind] {
			t.Fatalf("%s is watched and cached but also listed uncached; the cache and uncached-read sets must stay disjoint", kind)
		}
		if _, ok := wantUncached[kind]; !ok {
			t.Fatalf("unexpected uncached kind %s; only chart get/list-only kinds read by the secure path belong here", kind)
		}
		wantUncached[kind] = true
	}
	for kind, seen := range wantUncached {
		if !seen {
			t.Fatalf("kind %s is read by-name on the secure path but is not disabled from the cache; its first read would wedge a forbidden informer", kind)
		}
	}
}

func TestBuildSecureControlCarriesTokenReviewClusterRole(t *testing.T) {
	registryFile := filepath.Join(t.TempDir(), "providers.json")
	providers := `{"providers":[{"name":"github","type":"github","endpoint":"https://api.github.com/","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["*"]}]}`
	if err := os.WriteFile(registryFile, []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	observerClient, err := couriergithub.NewClient("https://api.github.com/", "operator-read-token", &recordingForgeTransport{})
	if err != nil {
		t.Fatal(err)
	}
	observer := couriergithub.Observer{Client: observerClient}
	control, err := buildSecureControl(secureConfig{
		providersFile:   registryFile,
		runNamespace:    "runs",
		observer:        observer,
		restConfig:      &rest.Config{Host: "https://127.0.0.1:1"},
		tokenReviewRole: "native-broker-tokenreview",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := control.Config.TokenReviewClusterRole; got != "native-broker-tokenreview" {
		t.Fatalf("TokenReviewClusterRole = %q, want the configured override", got)
	}
	defaulted, err := buildSecureControl(secureConfig{
		providersFile:   registryFile,
		runNamespace:    "runs",
		observer:        observer,
		restConfig:      &rest.Config{Host: "https://127.0.0.1:1"},
		tokenReviewRole: defaultBrokerTokenReviewClusterRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	if defaulted.Config.TokenReviewClusterRole != defaultBrokerTokenReviewClusterRole {
		t.Fatalf("TokenReviewClusterRole = %q, want the historical default", defaulted.Config.TokenReviewClusterRole)
	}
}

// The operator binds the ClusterRole rendered by secure.yaml.tpl by name, so
// the Go default and the template's fallback literal must never drift.
func TestBrokerTokenReviewDefaultMatchesChart(t *testing.T) {
	template, err := os.ReadFile(filepath.Join("..", "charts", "courier", "templates", "secure.yaml.tpl"))
	if err != nil {
		t.Fatalf("read secure template: %v", err)
	}
	if !strings.Contains(string(template), `default "`+defaultBrokerTokenReviewClusterRole+`"`) {
		t.Fatalf("secure.yaml.tpl must fall back to the operator default %q for the broker TokenReview ClusterRole", defaultBrokerTokenReviewClusterRole)
	}
	data, err := os.ReadFile(filepath.Join("..", "charts", "courier", "values.yaml"))
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	var values struct {
		Secure struct {
			BrokerTokenReviewClusterRole string `json:"brokerTokenReviewClusterRole"`
		} `json:"secure"`
		Ownership struct {
			Namespaces       []string `json:"namespaces"`
			LeaderElectionID string   `json:"leaderElectionID"`
		} `json:"ownership"`
	}
	if err := yaml.Unmarshal(data, &values); err != nil {
		t.Fatalf("parse chart values: %v", err)
	}
	if strings.TrimSpace(values.Secure.BrokerTokenReviewClusterRole) != "" {
		t.Fatal("chart default for secure.brokerTokenReviewClusterRole must stay empty so single releases keep the historical name")
	}
	if len(values.Ownership.Namespaces) != 0 || strings.TrimSpace(values.Ownership.LeaderElectionID) != "" {
		t.Fatal("chart defaults for ownership must stay empty so a single release keeps the backward-compatible boundary")
	}
}
