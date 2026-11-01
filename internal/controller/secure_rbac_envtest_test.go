package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// secureRBACProbeSeq numbers the probe namespaces. envtest runs no namespace
// controller, so a deleted namespace stays Terminating for the life of the
// control plane and its name can never be created again: each run of the
// test needs its own namespace to be repeatable inside one process.
var secureRBACProbeSeq atomic.Int64

// secureRBACProbeNamespace names the namespace one run of the probe holds.
func secureRBACProbeNamespace() string {
	return fmt.Sprintf("secure-rbac-probe-%d", secureRBACProbeSeq.Add(1))
}

// TestSecureReadsUnderNoWatchRBAC reproduces the #641 defect against a real
// API server and real authorization rather than a fake client: an operator
// whose chart RBAC grants get on Secrets and Services but never list/watch
// must still finish secure admission, preflight and provisioning, which
// point-read those kinds by name.
//
// It builds two managers over the same restrictive identity. The first carries
// the production uncached-read set, so a by-name read goes straight to the API
// server using only the get verb and returns the object. The second caches
// those kinds, so the same read lazily starts a cluster-wide informer that the
// authorization refuses; it never syncs, and the read is bounded only by its
// own context deadline — the wedge the fix removes. A missing grant, read
// through the uncached set, surfaces as an immediate Forbidden rather than a
// wedge, so the operator can report it.
func TestSecureReadsUnderNoWatchRBAC(t *testing.T) {
	if envtestServer == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	admin, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	// An isolated namespace holding the objects the secure path reads by name,
	// the service account the operator's identity impersonates, and a Role that
	// mirrors the shipped chart least-privilege: get only, never list/watch.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: secureRBACProbeNamespace()}}
	if err := admin.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Delete(context.Background(), ns) })

	for _, obj := range []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "signing-key"}, StringData: map[string]string{"signing.key": "placeholder"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "broker"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "tls", Port: 8443}}}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "operator"}},
	} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "operator-nocreate"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"secrets", "services"}, Verbs: []string{"get"}},
		},
	}
	if err := admin.Create(ctx, role); err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "operator-nocreate"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: ns.Name, Name: "operator"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name},
	}
	if err := admin.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	// The operator preflights by listing Nodes (cluster-scoped), so grant that
	// one cluster read without watch, mirroring the chart's nodes get+list.
	// The names carry the namespace, so each run of the test holds its own
	// cluster-scoped grants.
	nodeRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: ns.Name + "-nodes"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get", "list"}}},
	}
	if err := admin.Create(ctx, nodeRole); err != nil {
		t.Fatal(err)
	}
	nodeBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: ns.Name + "-nodes"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: ns.Name, Name: "operator"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: nodeRole.Name},
	}
	if err := admin.Create(ctx, nodeBinding); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = admin.Delete(context.Background(), nodeBinding)
		_ = admin.Delete(context.Background(), nodeRole)
	})

	// The operator's identity: impersonate the service account as an ordinary
	// authenticated subject so only the Role above authorizes its reads.
	operatorConfig := rest.CopyConfig(envtestServer.Config)
	operatorConfig.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + ns.Name + ":operator",
		Groups:   []string{"system:authenticated"},
	}

	secretKey := types.NamespacedName{Namespace: ns.Name, Name: "signing-key"}
	serviceKey := types.NamespacedName{Namespace: ns.Name, Name: "broker"}
	uncached := []client.Object{
		&corev1.Secret{}, &corev1.Service{}, &corev1.ServiceAccount{},
		&corev1.Namespace{}, &corev1.Node{}, &rbacv1.Role{},
	}

	t.Run("uncached reads succeed on the get verb alone", func(t *testing.T) {
		mgr := startManager(t, operatorConfig, scheme, uncached)
		secretCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		var got corev1.Secret
		if err := mgr.GetClient().Get(secretCtx, secretKey, &got); err != nil {
			t.Fatalf("uncached Secret read under get-only RBAC: %v", err)
		}
		if got.Name != "signing-key" {
			t.Fatalf("read Secret %q, want signing-key", got.Name)
		}
		var svc corev1.Service
		if err := mgr.GetClient().Get(secretCtx, serviceKey, &svc); err != nil {
			t.Fatalf("uncached Service read under get-only RBAC: %v", err)
		}
	})

	t.Run("uncached list succeeds on the list verb alone", func(t *testing.T) {
		// runLiveProbes lists Nodes, the one List the fix rescues. DisableFor
		// covers the NodeList read through its base Node object, so the list
		// goes to the API server needing only list — no watch informer starts.
		// (envtest runs no kubelet, so the list is legitimately empty.)
		mgr := startManager(t, operatorConfig, scheme, uncached)
		listCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		var nodes corev1.NodeList
		if err := mgr.GetClient().List(listCtx, &nodes); err != nil {
			t.Fatalf("uncached NodeList under get+list RBAC: %v", err)
		}
	})

	t.Run("missing grant surfaces as Forbidden, not a wedge", func(t *testing.T) {
		mgr := startManager(t, operatorConfig, scheme, uncached)
		// ServiceAccount is read uncached, and this identity has no get on it,
		// so the read returns a definitive authorization error instead of
		// blocking on an informer that can never sync.
		probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		var sa corev1.ServiceAccount
		err := mgr.GetClient().Get(probeCtx, types.NamespacedName{Namespace: ns.Name, Name: "operator"}, &sa)
		if !apierrors.IsForbidden(err) {
			t.Fatalf("uncached read of an ungranted kind err = %v, want Forbidden", err)
		}
	})

	t.Run("cached read cannot serve a get without list/watch", func(t *testing.T) {
		// Without the uncached-read set the same get-only identity cannot be
		// served: the read routes to the cache, which starts a cluster-wide
		// informer whose list/watch authorization refuses, so it never yields
		// the object and only the caller's cancellation ends the attempt — the wedge
		// #641 reports. The uncached read in the first subtest is the fix.
		//
		// The refusal is observed rather than waited out: the transport
		// reports the API server's 403 on the informer's Secret collection
		// read, so the test knows the informer can never sync and the read
		// is still blocked, without spending the wedge's whole duration.
		refused := make(chan struct{})
		wedgeConfig := rest.CopyConfig(operatorConfig)
		wedgeConfig.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
			return &secretRefusalTransport{next: next, refused: refused}
		}
		mgr := startManager(t, wedgeConfig, scheme, nil)
		wedgeCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		read := make(chan error, 1)
		var got corev1.Secret
		go func() {
			read <- mgr.GetClient().Get(wedgeCtx, secretKey, &got)
		}()
		select {
		case <-refused:
		case <-time.After(15 * time.Second):
			t.Fatal("the API server never refused the informer's Secret collection read: the fixture's get-only grants must refuse it")
		}
		// The informer was refused, so it can never sync and the cached read
		// cannot be served: the read is still blocked.
		select {
		case err := <-read:
			t.Fatalf("cached Secret read returned (%v) although the informer was refused", err)
		default:
		}
		// Only the caller's cancellation ends the attempt, and it ends it
		// without an object.
		cancel()
		select {
		case err := <-read:
			if err == nil {
				t.Fatal("cached Secret read unexpectedly returned without list/watch; the uncached-read set is what prevents this wedge")
			}
			// The read was blocked, not served: the cache reports the wait
			// for an informer that never synced, and the cancellation is
			// what released it.
			if !apierrors.IsTimeout(err) && !errors.Is(err, context.Canceled) {
				t.Fatalf("cached Secret read error = %v, want the blocked informer's timeout or the cancellation", err)
			}
			t.Logf("the cached read ended with %v and returned no object", err)
		case <-time.After(15 * time.Second):
			t.Fatal("cached Secret read did not return after the cancellation ended it")
		}
		if got.UID != "" {
			t.Fatalf("cached read returned object %q under denied list/watch, want no result", got.UID)
		}
	})
}

// secretRefusalTransport reports the API server's refusal of the Secret
// collection read the cached reader's informer issues. The get-only identity
// holds neither list nor watch on Secrets, so the refusal is what keeps the
// informer from ever syncing — and the cached read from ever being served.
type secretRefusalTransport struct {
	next    http.RoundTripper
	refused chan struct{}
	once    sync.Once
}

func (tr *secretRefusalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := tr.next.RoundTrip(req)
	if resp != nil && resp.StatusCode == http.StatusForbidden {
		call := classifyCall(req, resp)
		if call.resource == "secrets" && (call.verb == "list" || call.verb == "watch") {
			tr.once.Do(func() { close(tr.refused) })
		}
	}
	return resp, err
}

// managerReadyRunnable reports that the manager has started it, which it does
// only after the manager's caches are ready: the manager waits for its cache
// to sync before it runs anything else.
type managerReadyRunnable struct {
	ready chan struct{}
}

func (r *managerReadyRunnable) Start(ctx context.Context) error {
	close(r.ready)
	<-ctx.Done()
	return nil
}

// startManager builds and starts a controller manager with no reconcilers over
// the given configuration and cache read options, cancelling it when the test
// ends. It carries the same Client.Cache.DisableFor shape the operator wires in
// cmd/main.go so the test exercises real manager cache initialization.
//
// It returns once the manager has started its runnables: a manager that is
// still waiting for its cache to sync has not reached the point where it
// would notice a cancellation, so a read that starts an informer afterwards
// would wedge the manager's startup instead of the read alone.
func startManager(t *testing.T, config *rest.Config, scheme *runtime.Scheme, disableFor []client.Object) ctrl.Manager {
	t.Helper()
	options := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	}
	if disableFor != nil {
		options.Client = client.Options{Cache: &client.CacheOptions{DisableFor: disableFor}}
	}
	mgr, err := ctrl.NewManager(config, options)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	ready := &managerReadyRunnable{ready: make(chan struct{})}
	if err := mgr.Add(ready); err != nil {
		t.Fatalf("add the manager's readiness runnable: %v", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(runCtx) }()
	t.Cleanup(func() {
		cancel()
		// Drain Start's return on the test goroutine so a late stop is never
		// logged after the test has finished. Cancellation is the expected end.
		drained := time.Now()
		select {
		case err := <-started:
			t.Logf("the manager stopped %s after the cancellation", time.Since(drained).Round(time.Microsecond))
			if err != nil {
				t.Errorf("manager stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Logf("the manager did not stop within the 2s ceiling after the cancellation")
		}
	})
	select {
	case <-ready.ready:
	case err := <-started:
		cancel()
		t.Fatalf("the manager stopped before it started: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("the manager did not start")
	}
	if !mgr.GetCache().WaitForCacheSync(runCtx) {
		cancel()
		t.Fatal("manager cache did not sync")
	}
	return mgr
}
