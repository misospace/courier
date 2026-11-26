package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge/forgetest"
	"github.com/misospace/courier/internal/gitegress"
)

func TestEnvtestGitGuardRoutesCloneAndPushThroughService(t *testing.T) {
	if envtestServer == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	forgetest.RequireGit(t)
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	const laneName, runName = "gitguard-e2e-lane", "gitguard-e2e"
	if err := c.Create(ctx, envtestLane(laneName)); err != nil {
		t.Fatal(err)
	}
	run := envtestRun(runName, laneName)
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, envtestKey(runName), run); err != nil {
		t.Fatal(err)
	}
	run.Status.Branch = "courier/gitguard-e2e"
	if err := c.Status().Update(ctx, run); err != nil {
		t.Fatalf("set rendered work branch: %v", err)
	}
	launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "gitguard-e2e", expiresAt: time.Now().Add(time.Hour)}, time.Now(), plainExecutor{})
	launcher.GitGuardEgressCIDRs = []string{"140.82.112.0/20"}
	launcher.GitGuardKubernetesAPIIP = "10.96.0.1"
	if err := launcher.Launch(ctx, run); !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("first Launch: %v, want guard running gate", err)
	}
	var guardStarting corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, &guardStarting); err != nil {
		t.Fatal(err)
	}
	guardStarting.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: executor.GitEgressContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := c.Status().Update(ctx, &guardStarting); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(ctx, run); err != nil {
		t.Fatalf("second Launch: %v", err)
	}

	var service corev1.Service
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name)}, &service); err != nil {
		t.Fatal(err)
	}
	if len(service.Spec.Ports) == 0 {
		t.Fatal("guard Service has no ports")
	}
	port := service.Spec.Ports[0].Port
	if port != executor.GitEgressPort {
		t.Fatalf("guard Service port = %d, want %d", port, executor.GitEgressPort)
	}
	serviceHost := executor.GitGuardServiceDNS(run.Namespace, run.Name)
	// The DNS name must be composed from the provisioned Service's own name:
	// a GitGuardServiceDNS that drifted from GitGuardServiceName would render
	// a remote resolving to no Service at all, and only a comparison against
	// the stored object catches it.
	if dns := service.Name + "." + run.Namespace + ".svc.cluster.local"; serviceHost != dns {
		t.Fatalf("guard Service DNS = %q, want the provisioned Service's name %q", serviceHost, dns)
	}
	var guardPod, coordinator corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, &guardPod); err != nil {
		t.Fatal(err)
	}
	guardContainer := containerByName(t, guardPod.Spec.Containers, executor.GitEgressContainerName)
	baseIdentity := controllerEnvValue(guardContainer.Env, "COURIER_REPO")
	workIdentity := controllerEnvValue(guardContainer.Env, "COURIER_HEAD_REPO")
	if workIdentity == "" {
		workIdentity = baseIdentity
	}
	workRendered := controllerEnvValue(guardContainer.Env, "COURIER_REPO_URL")
	baseRendered := controllerEnvValue(guardContainer.Env, "COURIER_BASE_REPO_URL")
	if baseRendered == "" {
		// The guard binary falls back to the work remote when the base
		// rendering is empty (decideGitEgress); mirror it so a degenerate
		// render surfaces as production would, not as a path-less remote.
		baseRendered = workRendered
	}
	branch := controllerEnvValue(guardContainer.Env, "COURIER_BRANCH")
	if branch == "" {
		t.Fatal("guard COURIER_BRANCH is empty")
	}
	if branch != run.Status.Branch {
		t.Fatalf("guard COURIER_BRANCH = %q, want the run's resolved branch %q", branch, run.Status.Branch)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Name + "-coordinator"}, &coordinator); err != nil {
		t.Fatal(err)
	}
	if len(coordinator.Spec.Containers) == 0 {
		t.Fatal("coordinator Pod has no containers")
	}
	coordinatorURL := controllerEnvValue(coordinator.Spec.Containers[0].Env, "COURIER_REPO_URL")
	wantCoordinatorURL := fmt.Sprintf("http://%s:%d/acme/widgets.git", serviceHost, port)
	if coordinatorURL != wantCoordinatorURL {
		t.Fatalf("coordinator COURIER_REPO_URL = %q, want %q", coordinatorURL, wantCoordinatorURL)
	}

	forge := forgetest.NewSmartHTTPForge(t, "acme/widgets")
	workRemoteURL := remapRepositoryPath(t, workRendered, forge.UpstreamURL("acme/widgets"))
	remotes := []gitegress.Remote{{URL: workRemoteURL, Identity: workIdentity, Push: true}}
	if !strings.EqualFold(baseIdentity, workIdentity) {
		baseRemoteURL := remapRepositoryPath(t, baseRendered, forge.UpstreamURL("acme/widgets"))
		remotes = append(remotes, gitegress.Remote{URL: baseRemoteURL, Identity: baseIdentity})
	}
	// The rendered authority is github.com and intentionally unreachable here;
	// New admits loopback HTTP upstreams so the real guard can use this local forge.
	guard, err := gitegress.New(gitegress.Config{
		Remotes:     remotes,
		WorkRef:     "refs/heads/" + branch,
		Credentials: fixedGitGuardCredentials{identities: []string{baseIdentity, workIdentity}},
	})
	if err != nil {
		t.Fatalf("gitegress.New: %v", err)
	}
	guardServer := httptest.NewServer(guard)
	t.Cleanup(guardServer.Close)

	// Git sends absolute-URI requests through an HTTP proxy, so this bridge
	// preserves the coordinator's request line, Host, path, and body as kube-proxy would.
	var hostsMu sync.Mutex
	var hosts []string
	reverse := &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
		pr.SetURL(mustURL(t, guardServer.URL))
		pr.Out.Host = pr.In.Host
	}}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hostsMu.Lock()
		hosts = append(hosts, r.Host)
		hostsMu.Unlock()
		wantHost := fmt.Sprintf("%s:%d", serviceHost, port)
		if r.Host != wantHost {
			http.Error(w, "request Host did not name guard Service", http.StatusBadGateway)
			return
		}
		reverse.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	proxyURL := mustURL(t, proxy.URL)
	gitEnv := forge.Env()
	gitEnv = append(gitEnv,
		"http_proxy=http://"+proxyURL.Host,
		"no_proxy=",
		"NO_PROXY=",
	)
	cloneDir, err := os.MkdirTemp(forge.WorkDir(), "clone-acme-widgets-")
	if err != nil {
		t.Fatal(err)
	}
	forgetest.RunGit(t, gitEnv, forge.WorkDir(), "clone", coordinatorURL, cloneDir)
	seedTip := forge.Tip("acme/widgets", "refs/heads/main")
	if got := strings.TrimSpace(forgetest.RunGit(t, gitEnv, cloneDir, "rev-parse", "--verify", "HEAD")); got != seedTip {
		t.Fatalf("clone HEAD = %q, want seeded main %q", got, seedTip)
	}
	if _, err := os.Stat(filepath.Join(cloneDir, "seed.txt")); err != nil {
		t.Fatalf("seed.txt missing from clone: %v", err)
	}
	forgetest.RunGit(t, gitEnv, cloneDir, "checkout", "-B", branch)
	forgetest.CommitFile(t, gitEnv, cloneDir, "work.txt", "published through guard Service\n")
	pushed := strings.TrimSpace(forgetest.RunGit(t, gitEnv, cloneDir, "rev-parse", "--verify", "HEAD"))
	workRef := "refs/heads/" + branch
	forgetest.RunGit(t, gitEnv, cloneDir, "push", "origin", "HEAD:"+workRef)
	if got := forge.Tip("acme/widgets", workRef); got != pushed {
		t.Fatalf("work ref tip = %q, want pushed commit %q", got, pushed)
	}
	mainBefore := forge.Tip("acme/widgets", "refs/heads/main")
	forgetest.CommitFile(t, gitEnv, cloneDir, "main.txt", "must be refused\n")
	out, err := forgetest.TryGit(t, gitEnv, cloneDir, "push", "origin", "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push outside work ref succeeded:\n%s", out)
	}
	forgetest.AssertRefused(t, out)
	if got := forge.Tip("acme/widgets", "refs/heads/main"); got != mainBefore {
		t.Fatalf("main ref moved from %q to %q", mainBefore, got)
	}

	hostsMu.Lock()
	defer hostsMu.Unlock()
	wantHost := fmt.Sprintf("%s:%d", serviceHost, port)
	if len(hosts) == 0 {
		t.Fatal("forward proxy observed no requests")
	}
	for _, host := range hosts {
		if host != wantHost {
			t.Fatalf("proxy observed Host %q, want only %q", host, wantHost)
		}
	}
}

type fixedGitGuardCredentials struct {
	identities []string
}

func (c fixedGitGuardCredentials) Credentials(_ context.Context, identity string) (gitegress.Credential, error) {
	for _, pinned := range c.identities {
		if strings.EqualFold(identity, pinned) {
			return gitegress.Credential{Username: "x-access-token", Token: "gitguard-e2e"}, nil
		}
	}
	return gitegress.Credential{}, fmt.Errorf("credential identity %q is not pinned", identity)
}

func remapRepositoryPath(t *testing.T, rendered, upstream string) string {
	t.Helper()
	renderedURL, err := url.Parse(rendered)
	if err != nil {
		t.Fatalf("parse rendered remote %q: %v", rendered, err)
	}
	upstreamURL, err := url.Parse(upstream)
	if err != nil {
		t.Fatalf("parse forge upstream %q: %v", upstream, err)
	}
	upstreamURL.Path = renderedURL.Path
	return upstreamURL.String()
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URL %q: %v", raw, err)
	}
	return parsed
}

func containerByName(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, container := range containers {
		if container.Name == name {
			return container
		}
	}
	t.Fatalf("container %q not found", name)
	return corev1.Container{}
}
