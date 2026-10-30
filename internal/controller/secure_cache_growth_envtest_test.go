package controller

// The bounded-memory evidence for #642 lives here. The guard tests in
// secure_uncached_reads_test.go prove which client a read went through;
// these tests measure what that routing does to the operator: a real API
// server, the chart's RBAC for a non-admin operator identity, and the
// production cache configuration, so the provisioning path's memory and
// cache growth are measured rather than assumed. The measured path is the
// static-topology provisioning pass and the preflight read sites it
// reroutes; the live-probe phase is exercised by the guard test, whose
// uncached Node List is legitimate and therefore not what these tests
// assert on.

import (
	"context"
	"fmt"
	"net/http"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/cacheopts"
	"github.com/misospace/courier/internal/topology"
)

const (
	// cacheGrowthRunNamespace is the namespace the secure topology is
	// provisioned into.
	cacheGrowthRunNamespace = "courier-cache-growth"
	// cacheGrowthOperatorNamespace holds the operator's own credentials,
	// which the provisioning path copies into the run namespace.
	cacheGrowthOperatorNamespace = "courier-cache-growth-operator"
	// cacheGrowthOtherNamespace holds Secrets the run namespace does not
	// own, so a cluster-wide pull is distinguishable from a bounded one.
	cacheGrowthOtherNamespace = "courier-cache-growth-other"
	// cacheGrowthCacheService is the dependency cache Service the
	// provisioning path pins, so a Service read is on the measured path.
	cacheGrowthCacheService = "cache-service"
	// cacheGrowthOperatorUser is the identity the operator's clients
	// authenticate as. It is not an administrator: its grants are the
	// chart's operator ClusterRole, which lists the kinds the secure path
	// reads but never watches them.
	cacheGrowthOperatorUser = "courier-cache-growth-operator"
	// cacheGrowthSecretPopulation is how many Secrets the run namespace
	// holds. The population is what a cached Secret read pulls into the
	// operator's memory.
	cacheGrowthSecretPopulation = 64
	// cacheGrowthSecretPayload is the size of one seeded Secret.
	cacheGrowthSecretPayload = 64 << 10
	// cacheGrowthSecretsBytes is the size of the seeded population.
	cacheGrowthSecretsBytes = cacheGrowthSecretPopulation * cacheGrowthSecretPayload
	// cacheGrowthHeapBound is the growth the provisioning passes may leave
	// behind. It is half the seeded population: a pass that cached the
	// population would exceed it, a pass that caches nothing does not.
	cacheGrowthHeapBound = cacheGrowthSecretsBytes / 2
)

// cacheGrowthInformedKinds are the kinds the manager's cache
// informer-caches: the only kinds a list or a watch in the request record
// may name. Every other kind — the ones the chart grants list (or, for
// Namespaces, get) but never watch — is read point-wise through the
// uncached API reader.
var cacheGrowthInformedKinds = map[string]bool{
	"pods":         true,
	"coderruns":    true,
	"laneprofiles": true,
}

// apiCall is one request the operator's clients made to the API server.
type apiCall struct {
	verb      string
	resource  string
	namespace string
	name      string
	status    int
}

// requestRecorder records every request the operator's clients make. An
// informer always lists before it watches, so a kind with no list and no
// watch in the record was never cached. The record is the evidence the
// tests below assert on; nothing here infers cache state from a proxy for
// it.
type requestRecorder struct {
	mu    sync.Mutex
	calls []apiCall
}

// transport wraps a round tripper so every request is recorded.
func (r *requestRecorder) transport(next http.RoundTripper) http.RoundTripper {
	return &recordingTransport{recorder: r, next: next}
}

func (r *requestRecorder) record(call apiCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

// snapshot returns the requests recorded so far.
func (r *requestRecorder) snapshot() []apiCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]apiCall(nil), r.calls...)
}

// counted returns how many requests of a verb and resource were recorded.
func (r *requestRecorder) counted(verb, resource string) int {
	count := 0
	for _, call := range r.snapshot() {
		if call.verb == verb && call.resource == resource {
			count++
		}
	}
	return count
}

// listed reports whether a list of the resource was recorded, optionally
// within one namespace. A cluster-wide list carries no namespace.
func (r *requestRecorder) listed(resource, namespace string) bool {
	for _, call := range r.snapshot() {
		if call.verb == "list" && call.resource == resource && call.namespace == namespace {
			return true
		}
	}
	return false
}

// denied reports whether the API server refused a request of that verb and
// resource: the chart grants no watch on the kinds the secure path reads,
// so an informer that tries to watch one is refused.
func (r *requestRecorder) denied(verb, resource string) bool {
	for _, call := range r.snapshot() {
		if call.verb == verb && call.resource == resource && call.status == http.StatusForbidden {
			return true
		}
	}
	return false
}

type recordingTransport struct {
	recorder *requestRecorder
	next     http.RoundTripper
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	t.recorder.record(classifyCall(req, resp))
	return resp, err
}

// classifyCall names the API operation a request performs. The path shape
// tells a collection read (a list) from a named read (a get), and the watch
// parameter turns a collection read into a watch.
func classifyCall(req *http.Request, resp *http.Response) apiCall {
	resource, namespace, name := parseAPIPath(req.URL.Path)
	verb := strings.ToLower(req.Method)
	switch req.Method {
	case http.MethodGet:
		switch {
		case req.URL.Query().Get("watch") == "true":
			verb = "watch"
		case name == "":
			verb = "list"
		default:
			verb = "get"
		}
	case http.MethodPost:
		verb = "create"
	case http.MethodPut:
		verb = "update"
	case http.MethodPatch:
		verb = "patch"
	case http.MethodDelete:
		verb = "delete"
	}
	call := apiCall{verb: verb, resource: resource, namespace: namespace, name: name}
	if resource == "" {
		// A discovery request names no resource.
		call.verb = "discovery"
	}
	if resp != nil {
		call.status = resp.StatusCode
	}
	return call
}

// parseAPIPath splits an API server path into its resource, namespace and
// name. A namespaced collection is /api/v1/namespaces/{ns}/{resource}; a
// cluster-scoped one is /api/v1/{resource}. A subresource path is read as
// its parent resource, which the kinds asserted on never use.
func parseAPIPath(path string) (resource, namespace, name string) {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(segments) >= 2 && segments[0] == "api":
		segments = segments[2:]
	case len(segments) >= 3 && segments[0] == "apis":
		segments = segments[3:]
	default:
		return "", "", ""
	}
	if len(segments) == 0 {
		return "", "", ""
	}
	if segments[0] == "namespaces" {
		switch len(segments) {
		case 1:
			return "namespaces", "", ""
		case 2:
			return "namespaces", "", segments[1]
		default:
			namespace = segments[1]
			resource = segments[2]
			if len(segments) > 3 {
				name = segments[3]
			}
			return resource, namespace, name
		}
	}
	resource = segments[0]
	if len(segments) > 1 {
		name = segments[1]
	}
	return resource, namespace, name
}

// liveHeapBytes reports the live heap after a collection, so a test can
// measure what a pass left behind rather than what it allocated in total.
func liveHeapBytes() uint64 {
	goruntime.GC()
	var stats goruntime.MemStats
	goruntime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// heapGrowth reports how much live heap a measurement grew by. A collection
// between the two measurements can shrink the heap, which is growth of zero.
func heapGrowth(before, after uint64) uint64 {
	if after <= before {
		return 0
	}
	return after - before
}

// waitFor waits for a condition and fails the test with the given message if
// it does not hold in time. An informer works asynchronously, so the
// requests it makes are observed after the read that started it returns.
func waitFor(t *testing.T, condition func() bool, timeout time.Duration, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal(message)
}

// cacheGrowthCluster is the fixture the bounded-memory tests run against: a
// real API server holding a population of Secrets, the chart's grants for a
// non-admin operator identity, and the production cache configuration.
type cacheGrowthCluster struct {
	scheme   *k8sruntime.Scheme
	admin    client.Client
	base     *rest.Config
	recorder *requestRecorder
	run      *courier.CoderRun
}

// newCacheGrowthCluster seeds the cluster the tests measure against.
func newCacheGrowthCluster(t *testing.T, ctx context.Context) *cacheGrowthCluster {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	for _, add := range []func(*k8sruntime.Scheme) error{
		courier.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, networkingv1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	admin, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{cacheGrowthRunNamespace, cacheGrowthOperatorNamespace, cacheGrowthOtherNamespace} {
		createCacheGrowthNamespace(t, ctx, admin, namespace)
	}
	seedCacheGrowthSecrets(t, ctx, admin, cacheGrowthRunNamespace, "population", cacheGrowthSecretPopulation)
	seedCacheGrowthSecrets(t, ctx, admin, cacheGrowthOtherNamespace, "other", 4)
	// The operator's own forge credential: the provisioning path copies it
	// into the run namespace.
	credential := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "forge-creds", Namespace: cacheGrowthOperatorNamespace},
		Data:       map[string][]byte{"token": []byte("forge-token")},
	}
	if err := admin.Create(ctx, credential); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	// The dependency cache Service the provisioning path pins: a Service
	// read on the measured path.
	cacheService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: cacheGrowthCacheService, Namespace: cacheGrowthRunNamespace},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "http", Port: 8080}},
		},
	}
	if err := admin.Create(ctx, cacheService); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	grantCacheGrowthOperator(t, ctx, admin)
	// The chart's deployment-level ClusterRole the broker's TokenReview
	// binding points at (charts/courier/templates/secure.yaml.tpl).
	tokenReviewRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "courier-broker-tokenreview"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"authentication.k8s.io"}, Resources: []string{"tokenreviews"}, Verbs: []string{"create"}},
		},
	}
	if err := admin.Create(ctx, tokenReviewRole); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	// The chart grants the operator clusterrolebinding creation but not the
	// bind capability the API server's escalation check demands for the
	// broker's TokenReview binding (chart RBAC end to end is #641's scope).
	// The fixture grants it so the provisioning path completes; what this
	// test measures — the read grants the operator holds — mirrors the
	// chart exactly.
	binder := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "courier-cache-growth-tokenreview-binder"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"clusterroles"}, ResourceNames: []string{"courier-broker-tokenreview"}, Verbs: []string{"bind"}},
		},
	}
	if err := admin.Create(ctx, binder); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	bindBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "courier-cache-growth-tokenreview-binder"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: binder.Name},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, Name: cacheGrowthOperatorUser}},
	}
	if err := admin.Create(ctx, bindBinding); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}

	// Each test provisions its own client certificate for the same
	// identity: the RBAC binding is by name, so both certificates are the
	// same operator.
	user, err := envtestServer.AddUser(envtest.User{Name: cacheGrowthOperatorUser}, envtestServer.Config)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &requestRecorder{}
	return &cacheGrowthCluster{
		scheme:   scheme,
		admin:    admin,
		base:     user.Config(),
		recorder: recorder,
		run: &courier.CoderRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cache-growth-run",
				Namespace: cacheGrowthRunNamespace,
				UID:       types.UID("uid-cache-growth-run"),
			},
			Spec: courier.CoderRunSpec{
				Mode:       courier.ModeResolveIssue,
				Source:     "manual",
				WorkItemID: "item-1",
				Repo:       "acme/widgets",
				Ref:        7,
				Lane:       "local",
			},
		},
	}
}

// config returns a REST config for the operator identity whose requests are
// recorded by the given recorder.
func (c *cacheGrowthCluster) config(recorder *requestRecorder) *rest.Config {
	cfg := rest.CopyConfig(c.base)
	cfg.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
		return recorder.transport(next)
	}
	return cfg
}

// grantCacheGrowthOperator mirrors the chart's operator ClusterRole
// (charts/courier/values.yaml): list and watch for the three cached kinds,
// list without watch for the kinds the secure path reads, and get for
// Namespaces. The identity the tests authenticate as holds these grants, the
// bind grant below, and nothing else.
func grantCacheGrowthOperator(t *testing.T, ctx context.Context, admin client.Client) {
	t.Helper()
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "courier-cache-growth-operator"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"create", "delete", "get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "delete", "get", "list"}},
			{APIGroups: []string{""}, Resources: []string{"serviceaccounts"}, Verbs: []string{"create", "delete", "get", "list"}},
			{APIGroups: []string{""}, Resources: []string{"services"}, Verbs: []string{"create", "delete", "get", "list"}},
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"get"}},
			{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"networkpolicies"}, Verbs: []string{"create", "delete", "get", "list"}},
			{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"roles"}, Verbs: []string{"create", "delete", "get", "list"}},
			{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"rolebindings"}, Verbs: []string{"create", "delete", "get", "list"}},
			{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"clusterrolebindings"}, Verbs: []string{"create", "delete", "get", "list"}},
			{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"subjectaccessreviews", "selfsubjectaccessreviews"}, Verbs: []string{"create"}},
			{APIGroups: []string{"courier.misospace.dev"}, Resources: []string{"coderruns"}, Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
			{APIGroups: []string{"courier.misospace.dev"}, Resources: []string{"coderruns/finalizers"}, Verbs: []string{"update"}},
			{APIGroups: []string{"courier.misospace.dev"}, Resources: []string{"coderruns/status"}, Verbs: []string{"get", "patch", "update"}},
			{APIGroups: []string{"courier.misospace.dev"}, Resources: []string{"laneprofiles"}, Verbs: []string{"create", "get", "list", "patch", "update", "watch"}},
			{APIGroups: []string{"courier.misospace.dev"}, Resources: []string{"laneprofiles/status"}, Verbs: []string{"get", "patch", "update"}},
		},
	}
	if err := admin.Create(ctx, role); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "courier-cache-growth-operator"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, Name: cacheGrowthOperatorUser}},
	}
	if err := admin.Create(ctx, binding); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

func createCacheGrowthNamespace(t *testing.T, ctx context.Context, admin client.Client, name string) {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
	}}
	if err := admin.Create(ctx, namespace); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

// seedCacheGrowthSecrets fills a namespace with Secrets of a known size, so
// the memory a cached read of them costs is measurable.
func seedCacheGrowthSecrets(t *testing.T, ctx context.Context, admin client.Client, namespace, prefix string, count int) {
	t.Helper()
	payload := make([]byte, cacheGrowthSecretPayload)
	for i := range payload {
		payload[i] = byte(i)
	}
	for i := 0; i < count; i++ {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", prefix, i), Namespace: namespace},
			Data:       map[string][]byte{"payload": payload},
		}
		if err := admin.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
}

// newCache builds a controller-runtime cache over the cluster's API server
// with the given options and starts it.
func (c *cacheGrowthCluster) newCache(t *testing.T, ctx context.Context, options cache.Options) cache.Cache {
	t.Helper()
	return c.newCacheWith(t, ctx, c.config(c.recorder), options)
}

// newCacheWith builds a cache over a given REST config, so a test can hold
// two caches with different configurations over the same API server.
func (c *cacheGrowthCluster) newCacheWith(t *testing.T, ctx context.Context, cfg *rest.Config, options cache.Options) cache.Cache {
	t.Helper()
	options.Scheme = c.scheme
	cacheObj, err := cache.New(cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := cacheObj.Start(ctx); err != nil {
			t.Logf("cache stopped: %v", err)
		}
	}()
	if !cacheObj.WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}
	return cacheObj
}

// cachedClient builds the delegating client the manager hands out: reads of
// structured objects go through the cache, writes go straight to the API
// server.
func (c *cacheGrowthCluster) cachedClient(t *testing.T, cfg *rest.Config, cacheObj cache.Cache) client.Client {
	t.Helper()
	cached, err := client.New(cfg, client.Options{
		Scheme: c.scheme,
		Cache:  &client.CacheOptions{Reader: cacheObj},
	})
	if err != nil {
		t.Fatal(err)
	}
	return cached
}

// secureControl wires the secure control the way the manager wires it: the
// cached client for writes, the uncached API reader for point reads.
func (c *cacheGrowthCluster) secureControl(t *testing.T, cfg *rest.Config, cached client.Client) *SecureControl {
	t.Helper()
	apiReader, err := client.New(cfg, client.Options{Scheme: c.scheme})
	if err != nil {
		t.Fatal(err)
	}
	return &SecureControl{
		Client:    cached,
		APIReader: apiReader,
		Config: SecureConfig{
			RunNamespace:      cacheGrowthRunNamespace,
			Registry:          secureRegistry(),
			HarnessImage:      "harness:test",
			ProbeImage:        "busybox:test",
			LiveProbes:        false,
			OperatorNamespace: cacheGrowthOperatorNamespace,
			CacheService:      cacheGrowthRunNamespace + "/" + cacheGrowthCacheService,
			Providers:         fakeProviderFactory(secureForge()),
			Now:               func() time.Time { return time.Now() },
		},
	}
}

// TestSecureProvisioningKeepsRestrictedKindsOutOfTheCache measures the
// secure provisioning path against a real API server: the passes must read
// the kinds the chart denies watch as point reads, must start no informer
// for them, and must leave the operator's memory flat while the cluster
// holds a Secret population an informer would pull into it.
func TestSecureProvisioningKeepsRestrictedKindsOutOfTheCache(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := newCacheGrowthCluster(t, ctx)
	cacheObj := cluster.newCache(t, ctx, cacheopts.Options([]string{cacheGrowthRunNamespace}))
	cached := cluster.cachedClient(t, cluster.config(cluster.recorder), cacheObj)
	control := cluster.secureControl(t, cluster.config(cluster.recorder), cached)

	_, policy, err := control.ResolveAdmission(ctx, cluster.run)
	if err != nil {
		t.Fatalf("ResolveAdmission: %v", err)
	}
	// The persisted policy is what the secure path re-selects the
	// registration from.
	cluster.run.Status.PublicationPolicy = policy

	// Warm the transport so the measurement covers the provisioning passes
	// and not the client's first connection.
	warm := &corev1.Namespace{}
	if err := control.reader().Get(ctx, types.NamespacedName{Name: cacheGrowthRunNamespace}, warm); err != nil {
		t.Fatalf("warm-up read: %v", err)
	}

	baseline := liveHeapBytes()
	for pass := 0; pass < 3; pass++ {
		if err := control.provisionStatic(ctx, cluster.run, policy); err != nil {
			t.Fatalf("provisioning pass %d: %v", pass, err)
		}
		// The preflight read sites this change rerouted: a Namespace, a
		// Service, a Secret, and a ServiceAccount, each read point-wise
		// through the uncached reader.
		if err := control.checkNamespaceBoundary(ctx); err != nil {
			t.Fatalf("namespace boundary check, pass %d: %v", pass, err)
		}
		if _, err := control.cachePin(ctx); err != nil {
			t.Fatalf("cache pin, pass %d: %v", pass, err)
		}
		if _, err := control.renderedWorkerPublicKey(ctx, cluster.run); err != nil {
			t.Fatalf("worker public key, pass %d: %v", pass, err)
		}
		if _, err := control.serviceAccount(ctx, cluster.run, topology.ControlSAName(cluster.run.Name)); err != nil {
			t.Fatalf("service account read, pass %d: %v", pass, err)
		}
		// A legitimate uncached collection read, like the live probe's
		// cluster-wide Node List. It must not be reported as an informer:
		// only a watch means the cache started holding the kind.
		nodes := &corev1.NodeList{}
		if err := control.reader().List(ctx, nodes); err != nil {
			t.Fatalf("node list, pass %d: %v", pass, err)
		}
	}
	after := liveHeapBytes()
	growth := heapGrowth(baseline, after)
	t.Logf("three provisioning passes changed the live heap by %d bytes (bounded growth %d) and issued %d Secret reads; the cluster holds %d bytes of Secrets",
		int64(after)-int64(baseline), growth, cluster.recorder.counted("get", "secrets"), cacheGrowthSecretsBytes)

	// A watch is the informer's signature: an informer watches the kind it
	// caches, and nothing else does — the uncached API reader never
	// watches, so a collection read it issues is not an informer. A watch
	// on a kind the cache does not inform therefore means an informer
	// started for that kind and now holds every object of it in the
	// operator's memory.
	for _, call := range cluster.recorder.snapshot() {
		if call.verb != "watch" {
			continue
		}
		if cacheGrowthInformedKinds[call.resource] && call.namespace != "" {
			// The production cache informs the watched namespaces only.
			continue
		}
		t.Fatalf("provisioning issued a watch on %s: an informer for that kind started and holds every object of it in the operator's memory", call.resource)
	}
	// The passes must actually have read the restricted kinds, or the
	// assertion above would pass vacuously.
	if got := cluster.recorder.counted("get", "secrets"); got < 3 {
		t.Fatalf("the provisioning passes read %d Secrets, want one per pass: the informer assertion would pass vacuously", got)
	}
	if growth > cacheGrowthHeapBound {
		t.Fatalf("three provisioning passes grew the operator's live heap by %d bytes (bound %d) while the cluster holds %d bytes of Secrets: the passes must not cache them",
			growth, cacheGrowthHeapBound, cacheGrowthSecretsBytes)
	}
}

// TestCachedRestrictedKindReadPullsSecretsIntoTheCache reproduces the #642
// mechanism against the same cluster: a read of a kind the chart lists but
// never watches, through the cached client, starts an informer — and the
// informer lists, pulling every Secret of that kind into the operator's
// memory. Before this change the list was cluster-wide; the namespace bound
// the production cache now carries limits it to the watched namespaces.
// Neither replaces the routing fix: the informer exists at all only because
// a restricted kind was read through the cached client.
func TestCachedRestrictedKindReadPullsSecretsIntoTheCache(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := newCacheGrowthCluster(t, ctx)

	// The pre-change cache configuration: no namespace bound, so an
	// accidental Secret informer lists the whole cluster.
	unboundedRecorder := &requestRecorder{}
	unboundedCfg := cluster.config(unboundedRecorder)
	unbounded := cluster.newCacheWith(t, ctx, unboundedCfg, cache.Options{})
	unboundedClient := cluster.cachedClient(t, unboundedCfg, unbounded)
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cacheGrowthRunNamespace, Name: "population-0"}
	baseline := liveHeapBytes()
	if err := unboundedClient.Get(ctx, key, secret); err != nil {
		t.Fatalf("cached Secret read: %v", err)
	}
	growth := heapGrowth(baseline, liveHeapBytes())
	t.Logf("one cached Secret read grew the live heap by %d bytes", growth)
	if !unboundedRecorder.listed("secrets", "") {
		t.Fatal("the informer did not list Secrets cluster-wide")
	}
	cached := &corev1.SecretList{}
	if err := unbounded.List(ctx, cached); err != nil {
		t.Fatalf("list the cache: %v", err)
	}
	if len(cached.Items) < cacheGrowthSecretPopulation {
		t.Fatalf("the cache holds %d Secrets, want at least the %d the cluster holds: the informer pulled them into the operator's memory",
			len(cached.Items), cacheGrowthSecretPopulation)
	}
	if growth < cacheGrowthSecretsBytes/2 {
		t.Fatalf("the cached read grew the live heap by %d bytes, want at least half the %d bytes the population holds: the informer holds them in memory",
			growth, cacheGrowthSecretsBytes)
	}
	// The chart grants no watch on Secrets: the informer cannot watch, so it
	// lists again. The cache is rebuilt rather than maintained, which is
	// what made the cluster-wide pull an OOM.
	waitFor(t, func() bool { return unboundedRecorder.denied("watch", "secrets") }, 30*time.Second,
		"the informer was allowed to watch Secrets: the fixture's grants must mirror the chart, which grants no watch")
	waitFor(t, func() bool { return unboundedRecorder.counted("list", "secrets") >= 2 }, 30*time.Second,
		"the informer listed Secrets once: a denied watch must make it relist, which is the unbounded cache the #642 OOM came from")

	// The production configuration bounds such an informer to the watched
	// namespaces: the same cached read now lists one namespace.
	boundedRecorder := &requestRecorder{}
	boundedCfg := cluster.config(boundedRecorder)
	bounded := cluster.newCacheWith(t, ctx, boundedCfg, cacheopts.Options([]string{cacheGrowthRunNamespace}))
	boundedClient := cluster.cachedClient(t, boundedCfg, bounded)
	if err := boundedClient.Get(ctx, key, secret); err != nil {
		t.Fatalf("cached Secret read: %v", err)
	}
	if !boundedRecorder.listed("secrets", cacheGrowthRunNamespace) {
		t.Fatal("the informer did not list Secrets in the watched namespace")
	}
	if boundedRecorder.listed("secrets", "") {
		t.Fatal("the bounded cache listed Secrets cluster-wide")
	}
	boundedSecrets := &corev1.SecretList{}
	if err := bounded.List(ctx, boundedSecrets); err != nil {
		t.Fatalf("list the cache: %v", err)
	}
	for i := range boundedSecrets.Items {
		if boundedSecrets.Items[i].Namespace != cacheGrowthRunNamespace {
			t.Fatalf("the bounded cache holds a Secret from %q, want only the watched namespace", boundedSecrets.Items[i].Namespace)
		}
	}
}
