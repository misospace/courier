package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// errCachedInformerRead is the sentinel any cached-client read of a kind the
// chart deliberately denies list/watch must fail with. The operator caches
// only CoderRun, LaneProfile and Pod; every other kind must be read through
// the uncached API reader, because a read through the cached client lazily
// starts an informer the RBAC cannot sustain — it lists the kind and can
// never watch, so it relists forever while holding every object it listed
// in the operator's memory. That is the OOM this enforcement exists to
// prevent (#642); the measured form of it is in
// secure_cache_growth_envtest_test.go.
var errCachedInformerRead = errors.New("cached informer read: restricted kind must be read through the uncached API reader")

// cachedInformerGuard wraps the cached client and fails every Get or List of
// a restricted kind, so a provisioning pass that touches one through the
// cached client cannot pass silently: the fake client never starts informers,
// meaning only this guard can catch the lazy-informer vector the real cluster
// exhibits as an unbounded cache.
type cachedInformerGuard struct {
	client.Client
}

func cachedInformerReadRestricted(obj client.Object) bool {
	switch obj.(type) {
	case *corev1.Secret, *corev1.Service, *corev1.Namespace,
		*corev1.ServiceAccount, *corev1.Node, *rbacv1.Role,
		*rbacv1.RoleBinding, *corev1.ConfigMap:
		return true
	}
	return false
}

func cachedInformerListRestricted(list client.ObjectList) bool {
	switch list.(type) {
	case *corev1.SecretList, *corev1.ServiceList, *corev1.NamespaceList,
		*corev1.ServiceAccountList, *corev1.NodeList, *rbacv1.RoleList,
		*rbacv1.RoleBindingList, *corev1.ConfigMapList:
		return true
	}
	return false
}

func (g cachedInformerGuard) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if cachedInformerReadRestricted(obj) {
		return errCachedInformerRead
	}
	return g.Client.Get(ctx, key, obj, opts...)
}

func (g cachedInformerGuard) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if cachedInformerListRestricted(list) {
		return errCachedInformerRead
	}
	return g.Client.List(ctx, list, opts...)
}

// uncachedSecureControl splits the fake client into the two readers the
// manager wires in production: the cached client wrapped so any restricted
// kind read fails closed, and the plain fake serving the API reader role the
// point reads must go through.
func uncachedSecureControl(t *testing.T, control *SecureControl, c client.Client) {
	t.Helper()
	control.Client = cachedInformerGuard{Client: c}
	control.APIReader = c
}

// TestSecureProvisionReadsRestrictedKindsUncached drives one full
// provisioning pass — static topology, preflight, cache pinning — through a
// cached client that refuses every restricted-kind read. The pass must
// complete with no sentinel error: every Secret, Service, Namespace, Role and
// ServiceAccount read on the launch path goes through the uncached API
// reader, so no lazy informer can start for a kind the chart grants no
// list/watch.
func TestSecureProvisionReadsRestrictedKindsUncached(t *testing.T) {
	ctx := context.Background()
	control, c := secureControl(t)
	run := launchedSecureRun(t, c, "uncached-provision", "local")
	uncachedSecureControl(t, control, c)

	result, err := control.LaunchSecure(ctx, run)
	if err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	if result.Requeue || result.RequeueAfter > 0 {
		t.Fatalf("provisioning returned a requeue: %#v", result)
	}
	if !topologyPodsExistCached(ctx, t, c, run) {
		t.Fatal("provisioning did not create the secure topology pods")
	}
}

// TestSecureProvisioningPassesStayUncached mimics the 2s requeue loop: every
// provisioning pass re-reads the restricted kinds (the cache pin, the broker
// TLS and credential Secrets, the policy Secret, the run namespace, the
// broker Role), and every pass must keep those reads off the cached client.
// A single uncached first pass would still OOM if a later pass drifted back
// onto the cache.
func TestSecureProvisioningPassesStayUncached(t *testing.T) {
	ctx := context.Background()
	control, c := secureControl(t)
	run := launchedSecureRun(t, c, "uncached-passes", "local")
	uncachedSecureControl(t, control, c)

	for pass := 0; pass < 5; pass++ {
		result, err := control.LaunchSecure(ctx, run)
		if err != nil {
			t.Fatalf("provisioning pass %d: %v", pass, err)
		}
		if result.Requeue || result.RequeueAfter > 0 {
			t.Fatalf("provisioning pass %d returned a requeue: %#v", pass, result)
		}
	}
}

// TestSecureLiveProbesReadNodesUncached covers the Node List and the three
// Service reads of the live-probe phase: the probes must list Nodes and
// resolve the API server, DNS, and broker Services through the uncached
// reader, because the chart grants list/watch for none of those kinds. The
// probes themselves cannot complete against the fake cluster, so the pass
// ends at its timeout — the assertion is that no restricted read tripped the
// guard on the way there.
func TestSecureLiveProbesReadNodesUncached(t *testing.T) {
	ctx := context.Background()
	control, c := secureControl(t)
	run := launchedSecureRun(t, c, "uncached-probes", "local")
	uncachedSecureControl(t, control, c)
	control.Config.LiveProbes = true
	control.Config.ProbeTimeout = 50 * time.Millisecond

	node := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	node.Name = "probe-node"
	if err := c.Create(ctx, node); err != nil {
		t.Fatal(err)
	}
	// provisionStatic tolerates AlreadyExists, so the broker Service is
	// seeded with a ClusterIP the fake create would not assign and the
	// probe-phase read resolves it.
	brokerService := topology.BrokerService(run)
	brokerService.Spec.ClusterIP = "10.0.0.3"
	if err := c.Create(ctx, brokerService); err != nil {
		t.Fatal(err)
	}
	for _, service := range []*corev1.Service{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "kubernetes"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-dns"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.2"}},
	} {
		if err := c.Create(ctx, service); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := control.LaunchSecure(ctx, run); err == nil {
		t.Fatal("live probes cannot complete against the fake cluster, want the timeout error")
	} else if errors.Is(err, errCachedInformerRead) {
		t.Fatalf("a restricted kind was read through the cached client during live probes: %v", err)
	} else if !strings.Contains(err.Error(), "did not complete in time") {
		t.Fatalf("live probes failed for an unexpected reason before the probe timeout: %v", err)
	}
}

// TestSecureForbiddenSecretReadRequeues is the missing-permission failure
// path: an API reader whose Secret reads come back Forbidden must fail the
// provisioning pass with a clear error and requeue the run — never a silent
// success and never a NeedsHuman terminalization, because a permission that
// can be granted later is a requeue, not a permanent fault.
func TestSecureForbiddenSecretReadRequeues(t *testing.T) {
	ctx := context.Background()
	control, c := secureControl(t)
	run := launchedSecureRun(t, c, "forbidden-secret", "local")
	control.Client = cachedInformerGuard{Client: c}
	control.APIReader = forbiddenSecretReader{Reader: c}

	_, err := control.LaunchSecure(ctx, run)
	if err == nil {
		t.Fatal("a Forbidden Secret read must fail provisioning, want an error")
	}
	if errors.Is(err, errCachedInformerRead) {
		t.Fatalf("the Forbidden surfaced through the cached client, not the API reader: %v", err)
	}
	var forbidden *apierrors.StatusError
	if !errors.As(err, &forbidden) || !apierrors.IsForbidden(err) {
		t.Fatalf("provisioning error = %v, want it to carry the Forbidden anywhere in its chain", err)
	}

	var updated courier.CoderRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(run), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase == courier.PhaseNeedsHuman {
		t.Fatal("a Forbidden Secret read is transient and must not terminalize the run NeedsHuman")
	}
}

// forbiddenSecretReader stands in for an API reader whose Secret reads the
// API server denies, isolating the failure classification from the rest of
// the point-read surface.
type forbiddenSecretReader struct {
	client.Reader
}

func (r forbiddenSecretReader) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		return apierrors.NewForbidden(
			schema.GroupResource{Group: "", Resource: "secrets"},
			key.Name, errors.New("the operator's service account may not read secrets here"))
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func topologyPodsExistCached(ctx context.Context, t *testing.T, c client.Client, run *courier.CoderRun) bool {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	components := map[string]bool{}
	for i := range pods.Items {
		components[pods.Items[i].Labels["courier.misospace.dev/component"]] = true
	}
	return components[topology.ComponentCoordinator] &&
		components[topology.ComponentWorker] &&
		components[topology.ComponentBroker]
}
