package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
)

// These tests drive the broker RBAC preflight against a real API server with
// the RBAC authorizer enabled (envtest defaults to --authorization-mode=RBAC),
// so the decisions the preflight acts on are the API server's own. They cover
// #644: the broker's pod reads are name-scoped to the run's three topology
// pods, so an unrestricted get review must not stand in for the named grants.

func rbacTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = courier.AddToScheme(scheme)
	_ = rbacv1.AddToScheme(scheme)
	return scheme
}

// startBrokerRBACRun creates a run, reads it back for its UID, and returns a
// SecureControl wired to the envtest API server for both the Role read and the
// access reviews.
func startBrokerRBACRun(t *testing.T, name string) (ctrlclient.Client, *SecureControl, *courier.CoderRun) {
	t.Helper()
	cfg := envtestServer.Config
	c, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: rbacTestScheme()})
	if err != nil {
		t.Fatalf("envtest client: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("envtest clientset: %v", err)
	}
	run := envtestRun(name, "rbac-lane")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), run) })
	fetched := &courier.CoderRun{}
	if err := c.Get(context.Background(), ctrlclient.ObjectKeyFromObject(run), fetched); err != nil {
		t.Fatalf("get run: %v", err)
	}
	control := &SecureControl{
		Client: c,
		Config: SecureConfig{RunNamespace: fetched.Namespace, Kubernetes: clientset},
	}
	return c, control, fetched
}

func applyBrokerRole(t *testing.T, c ctrlclient.Client, run *courier.CoderRun, role *rbacv1.Role) {
	t.Helper()
	if err := c.Create(context.Background(), role); err != nil {
		t.Fatalf("create broker Role: %v", err)
	}
	if err := c.Create(context.Background(), topology.BrokerRoleBinding(run)); err != nil {
		t.Fatalf("create broker RoleBinding: %v", err)
	}
}

// grantBrokerTokenReviewCluster provisions the deployment-scoped ClusterRole
// that carries only TokenReview creation and binds it to the run's broker
// service account, mirroring the chart's contract.
func grantBrokerTokenReviewCluster(t *testing.T, c ctrlclient.Client, run *courier.CoderRun) {
	t.Helper()
	name := run.Name + "-tokenreviews"
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"tokenreviews"},
			Verbs:     []string{"create"},
		}},
	}
	if err := c.Create(context.Background(), clusterRole); err != nil {
		t.Fatalf("create tokenreviews ClusterRole: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), clusterRole) })
	binding := topology.BrokerTokenReviewBinding(run, name)
	if err := c.Create(context.Background(), binding); err != nil {
		t.Fatalf("create tokenreviews ClusterRoleBinding: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), binding) })
}

// brokerReadAllowed reports whether the named subjectAccessReviewAs is allowed,
// so tests can poll a grant through the API server's RBAC informer sync.
func brokerReadAllowed(control *SecureControl, broker string, review accessReview) (bool, error) {
	return control.subjectAccessReviewAs(context.Background(), broker, review)
}

// eventually polls check until it passes or a deadline lapses, so a freshly
// applied binding has propagated through the API server's RBAC informers.
func eventually(t *testing.T, check func() error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for {
		if last = check(); last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met before deadline: %v", last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestPreflightBrokerRBACNamedGrantsEnvtest proves the broker RBAC check
// against a real RBAC authorizer: each exact rendered pod name is readable, an
// unrestricted or unrelated get is denied, list is allowed, TokenReview is
// allowed via the cluster grant, and the full check passes.
func TestPreflightBrokerRBACNamedGrantsEnvtest(t *testing.T) {
	if envtestServer == nil || envtestServer.Config == nil {
		t.Skip("envtest is not available")
	}
	c, control, run := startBrokerRBACRun(t, "rbac-named")
	applyBrokerRole(t, c, run, topology.BrokerRole(run))
	grantBrokerTokenReviewCluster(t, c, run)

	broker := topology.BrokerSAName(run.Name)
	eventually(t, func() error {
		for _, review := range []accessReview{
			{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name)},
			{group: "", resource: "pods", verb: "get", name: topology.WorkerPodName(run.Name)},
			{group: "", resource: "pods", verb: "get", name: topology.BrokerPodName(run.Name)},
			{group: "", resource: "pods", verb: "list"},
			{group: "authentication.k8s.io", resource: "tokenreviews", verb: "create", clusterScoped: true},
		} {
			allowed, err := brokerReadAllowed(control, broker, review)
			if err != nil {
				return err
			}
			if !allowed {
				return errors.New(review.verb + " " + review.resource + " " + review.name + " expected allowed")
			}
		}
		return control.checkBrokerRBAC(context.Background(), run)
	})

	// The Role grants get pods only by name: an unrestricted (unnamed) get and
	// a get on an unrelated pod must be denied. These never become allowed, so
	// they hold once the positive reads above synced.
	for _, review := range []accessReview{
		{group: "", resource: "pods", verb: "get"},
		{group: "", resource: "pods", verb: "get", name: "unrelated-pod"},
	} {
		allowed, err := brokerReadAllowed(control, broker, review)
		if err != nil {
			t.Fatalf("access review: %v", err)
		}
		if allowed {
			t.Fatalf("unnamed/unrelated get pods (name %q) must be denied by the name-scoped Role", review.name)
		}
	}
}

// TestPreflightBrokerRBACMissingNamedGrantFailsEnvtest proves the named reads
// are individual against a real authorizer: a Role that omits one topology pod
// name denies just that pod while the others stay readable. The full broker RBAC
// check still refuses such a run (here on the Role-vs-intent comparison, which
// precedes the reviews); that checkBrokerRBAC issues a review per pod name is
// pinned separately by TestCheckBrokerRBACReviewScoping.
func TestPreflightBrokerRBACMissingNamedGrantFailsEnvtest(t *testing.T) {
	if envtestServer == nil || envtestServer.Config == nil {
		t.Skip("envtest is not available")
	}
	c, control, run := startBrokerRBACRun(t, "rbac-missing")

	partial := topology.BrokerRole(run).DeepCopy()
	for i := range partial.Rules {
		rule := &partial.Rules[i]
		if len(rule.ResourceNames) == 3 {
			// Keep control and broker; drop the worker name.
			rule.ResourceNames = []string{
				topology.ControlPodName(run.Name),
				topology.BrokerPodName(run.Name),
			}
		}
	}
	applyBrokerRole(t, c, run, partial)

	broker := topology.BrokerSAName(run.Name)
	// Once the control pod read is allowed the Role has propagated, so a worker
	// denial there is the name-scope, not a sync lag.
	eventually(t, func() error {
		allowed, err := brokerReadAllowed(control, broker,
			accessReview{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name)})
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("control pod read not yet allowed")
		}
		return nil
	})
	if allowed, err := brokerReadAllowed(control, broker,
		accessReview{group: "", resource: "pods", verb: "get", name: topology.WorkerPodName(run.Name)}); err != nil {
		t.Fatalf("worker read review: %v", err)
	} else if allowed {
		t.Fatal("worker pod get must be denied when its name is not in the Role's resourceNames")
	}

	var permanent *SecureNeedsHumanError
	err := control.checkBrokerRBAC(context.Background(), run)
	if !errors.As(err, &permanent) || permanent.Reason != "BrokerRBAC" {
		t.Fatalf("checkBrokerRBAC error = %v, want a BrokerRBAC needs-human verdict", err)
	}
}

// TestPreflightBrokerRBACTokenReviewNotNamespacedEnvtest proves the TokenReview
// review is cluster-scoped against a real authorizer: a TokenReview grant
// delivered only through a namespaced Role must not satisfy the cluster-scoped
// review, so the check refuses the run on tokenreviews even though every named
// pod read is allowed.
func TestPreflightBrokerRBACTokenReviewNotNamespacedEnvtest(t *testing.T) {
	if envtestServer == nil || envtestServer.Config == nil {
		t.Skip("envtest is not available")
	}
	c, control, run := startBrokerRBACRun(t, "rbac-tr-namespaced")
	applyBrokerRole(t, c, run, topology.BrokerRole(run))

	// TokenReview granted only in the run namespace: a namespaced Role can
	// never authorize a cluster-scoped create.
	roleName := run.Name + "-tr-namespaced"
	nsRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: roleName},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"tokenreviews"},
			Verbs:     []string{"create"},
		}},
	}
	if err := c.Create(context.Background(), nsRole); err != nil {
		t.Fatalf("create namespaced tokenreviews Role: %v", err)
	}
	if err := c.Create(context.Background(), &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: roleName},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: roleName},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      topology.BrokerSAName(run.Name),
			Namespace: run.Namespace,
		}},
	}); err != nil {
		t.Fatalf("create namespaced tokenreviews RoleBinding: %v", err)
	}

	broker := topology.BrokerSAName(run.Name)
	// Once the control pod read is allowed the whole Role (bound atomically via
	// its RoleBinding) is effective, so every review before TokenReview passes
	// and any failure must be the cluster-scoped TokenReview itself.
	eventually(t, func() error {
		allowed, err := brokerReadAllowed(control, broker,
			accessReview{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name)})
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("control pod read not yet allowed")
		}
		return nil
	})

	var permanent *SecureNeedsHumanError
	err := control.checkBrokerRBAC(context.Background(), run)
	if err == nil {
		t.Fatal("a namespaced-only TokenReview grant must not satisfy the cluster-scoped review")
	}
	if !errors.As(err, &permanent) || permanent.Reason != "BrokerRBAC" || !strings.Contains(err.Error(), "tokenreviews") {
		t.Fatalf("checkBrokerRBAC error = %v, want a BrokerRBAC verdict on tokenreviews", err)
	}
}
