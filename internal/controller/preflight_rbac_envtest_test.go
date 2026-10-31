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
	"k8s.io/client-go/rest"
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
		Config: SecureConfig{
			RunNamespace:      fetched.Namespace,
			OperatorNamespace: "courier-system",
			Kubernetes:        clientset,
		},
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
			{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name), namespace: run.Namespace},
			{group: "", resource: "pods", verb: "get", name: topology.WorkerPodName(run.Name), namespace: run.Namespace},
			{group: "", resource: "pods", verb: "get", name: topology.BrokerPodName(run.Name), namespace: run.Namespace},
			{group: "", resource: "pods", verb: "list", namespace: run.Namespace},
			// TokenReview creation is cluster-scoped: the review carries no
			// namespace so only a cluster-wide grant satisfies it (#644).
			{group: "authentication.k8s.io", resource: "tokenreviews", verb: "create"},
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
		{group: "", resource: "pods", verb: "get", namespace: run.Namespace},
		{group: "", resource: "pods", verb: "get", name: "unrelated-pod", namespace: run.Namespace},
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
			accessReview{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name), namespace: run.Namespace})
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("control pod read not yet allowed")
		}
		return nil
	})
	if allowed, err := brokerReadAllowed(control, broker,
		accessReview{group: "", resource: "pods", verb: "get", name: topology.WorkerPodName(run.Name), namespace: run.Namespace}); err != nil {
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
			accessReview{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name), namespace: run.Namespace})
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

// TestPreflightOperatorClusterScopedReviewNotNamespacedEnvtest proves the
// scoping rule the operator's grant proof rests on, against a real RBAC
// authorizer.
//
// A namespaced Role may name a cluster-scoped resource: RBAC does not police
// the scope of the resource a rule names, and the authorizer honours the
// grant for any request that names the role's namespace. A cluster-scoped
// request carries no namespace, so only a cluster-wide grant authorizes it.
// The operator's reviews therefore have to ask the cluster-scoped question:
// a review that named the run namespace would credit the operator with a
// grant its real request would be refused by, and the missing cluster grant
// would read as present (#650).
//
// The subject asked here is an explicit user with no groups, as a
// SubjectAccessReview for a named subject is. The SelfSubjectAccessReview the
// operator asks about itself resolves the requester's real groups, where the
// API server's default `system:basic-user` binding already allows the create;
// the scoping under test is the same in both forms.
func TestPreflightOperatorClusterScopedReviewNotNamespacedEnvtest(t *testing.T) {
	if envtestServer == nil || envtestServer.Config == nil {
		t.Skip("envtest is not available")
	}
	c, control, run := startBrokerRBACRun(t, "rbac-operator-scope")

	serviceAccount := run.Name + "-scope-sa"
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: serviceAccount},
	}
	if err := c.Create(context.Background(), sa); err != nil {
		t.Fatalf("create service account: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), sa) })
	// One namespaced Role granting every cluster-scoped verb the operator's
	// own reviews ask for: the grants are real, and only their scope is wrong.
	roleName := run.Name + "-cluster-scoped-grants"
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: roleName},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"list"}},
			{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"get"}},
			{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"clusterrolebindings"}, Verbs: []string{"create"}},
			{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"selfsubjectaccessreviews", "subjectaccessreviews"}, Verbs: []string{"create"}},
		},
	}
	if err := c.Create(context.Background(), role); err != nil {
		t.Fatalf("create namespaced cluster-scoped Role: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), role) })
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: roleName},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: roleName},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount,
			Namespace: run.Namespace,
		}},
	}
	if err := c.Create(context.Background(), binding); err != nil {
		t.Fatalf("create namespaced cluster-scoped RoleBinding: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), binding) })

	// The production review set, asked for the subject that holds only those
	// namespaced grants.
	clusterScoped := []accessReview{}
	for _, review := range control.operatorAccessReviews(run) {
		if review.namespace == "" {
			clusterScoped = append(clusterScoped, review)
		}
	}
	if len(clusterScoped) == 0 {
		t.Fatal("the operator's review set has no cluster-scoped review, so nothing proves the scoping")
	}

	// Once a namespaced review is allowed the Role has propagated through the
	// API server's RBAC informers, so the cluster-scoped denials below are the
	// scope and not a sync lag.
	eventually(t, func() error {
		allowed, err := control.subjectAccessReviewAs(context.Background(), serviceAccount,
			accessReview{group: "", resource: "nodes", verb: "list", namespace: run.Namespace})
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("namespaced nodes list not yet allowed")
		}
		return nil
	})

	for _, review := range clusterScoped {
		allowed, err := control.subjectAccessReviewAs(context.Background(), serviceAccount, review)
		if err != nil {
			t.Fatalf("cluster-scoped %s review: %v", review.resource, err)
		}
		if allowed {
			t.Errorf("a namespaced Role must not satisfy the cluster-scoped %s %s review", review.verb, review.resource)
		}
		// The same grant, asked in the run namespace, is allowed: the denial
		// above is about the scope of the grant, not a missing one.
		namespaced := review
		namespaced.namespace = run.Namespace
		allowed, err = control.subjectAccessReviewAs(context.Background(), serviceAccount, namespaced)
		if err != nil {
			t.Fatalf("namespaced %s review: %v", review.resource, err)
		}
		if !allowed {
			t.Errorf("the namespaced %s %s grant must be allowed in namespace %q", review.verb, review.resource, run.Namespace)
		}
	}
}

// TestPreflightOperatorReadGrantsNamespaceScopedEnvtest proves the operator's
// read-grant coverage against a real RBAC authorizer (#655).
//
// The secure launch path reads objects outside the set the provisioning proof
// covers, and one of those reads targets the operator's own namespace rather
// than the run's: the credential source secret. A grant that exists in the run
// namespace must not satisfy a review asked in another namespace — the RBAC
// authorizer evaluates the namespace literally — so the review for that read
// is asked where the read happens, and a deployment whose only secrets-get
// grant is scoped to the run namespace is refused here, with the OperatorRBAC
// verdict the caller sees, before the operator creates anything.
//
// The subject asked here is the operator's own identity as the API server
// resolves it: the clientset impersonates the provisioning service account,
// so the SelfSubjectAccessReviews checkOperatorAccess issues are answered for
// that account's real grants.
func TestPreflightOperatorReadGrantsNamespaceScopedEnvtest(t *testing.T) {
	if envtestServer == nil || envtestServer.Config == nil {
		t.Skip("envtest is not available")
	}
	// A dedicated run namespace, so "default" and "kube-system" stay genuinely
	// outside it: the default envtest run namespace is "default", which would
	// collapse the cross-namespace distinctions under test.
	const (
		runNamespace      = "rbac-operator-reads"
		operatorNamespace = "courier-system"
	)
	c, err := ctrlclient.New(envtestServer.Config, ctrlclient.Options{Scheme: rbacTestScheme()})
	if err != nil {
		t.Fatalf("envtest client: %v", err)
	}
	if err := c.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: runNamespace}}); err != nil {
		t.Fatalf("create run namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: runNamespace}})
	})
	if err := c.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: operatorNamespace}}); err != nil {
		t.Fatalf("create operator namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: operatorNamespace}})
	})

	run := envtestRunInNamespace("rbac-operator-reads", "rbac-lane", runNamespace)
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), run) })
	fetched := &courier.CoderRun{}
	if err := c.Get(context.Background(), ctrlclient.ObjectKeyFromObject(run), fetched); err != nil {
		t.Fatalf("get run: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(envtestServer.Config)
	if err != nil {
		t.Fatalf("envtest clientset: %v", err)
	}
	control := &SecureControl{
		Client: c,
		Config: SecureConfig{
			RunNamespace:      runNamespace,
			OperatorNamespace: "courier-system",
			Kubernetes:        clientset,
		},
	}

	serviceAccount := run.Name + "-operator-reads"
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: serviceAccount},
	}
	if err := c.Create(context.Background(), sa); err != nil {
		t.Fatalf("create service account: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), sa) })

	// The provisioning grants, delivered cluster-wide as the chart's
	// rbac.roles.operator ClusterRole does — but read grants deliberately
	// excluded, so the only secrets/services get the subject holds come from
	// the run-namespace Role below.
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: run.Name + "-operator-provisioning"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"secrets", "serviceaccounts", "pods", "services"}, Verbs: []string{"create", "delete"}},
			{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"networkpolicies"}, Verbs: []string{"create", "delete"}},
			{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"roles", "rolebindings", "clusterrolebindings"}, Verbs: []string{"create"}},
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"list"}},
			{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"get"}},
			{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"selfsubjectaccessreviews", "subjectaccessreviews"}, Verbs: []string{"create"}},
		},
	}
	if err := c.Create(context.Background(), clusterRole); err != nil {
		t.Fatalf("create provisioning ClusterRole: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), clusterRole) })
	clusterBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: clusterRole.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: clusterRole.Name},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount,
			Namespace: run.Namespace,
		}},
	}
	if err := c.Create(context.Background(), clusterBinding); err != nil {
		t.Fatalf("create provisioning ClusterRoleBinding: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), clusterBinding) })

	// The run-namespace read grants: what the secure launch path reads there.
	readRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: run.Name + "-operator-reads"},
		Rules: []rbacv1.PolicyRule{
			// pods list mirrors the cached list topologyPodsExist issues; the
			// gets cover the named reads the proof pins.
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{""}, Resources: []string{"secrets", "services"}, Verbs: []string{"get"}},
			{APIGroups: []string{courier.GroupVersion.Group}, Resources: []string{"laneprofiles"}, Verbs: []string{"get"}},
		},
	}
	if err := c.Create(context.Background(), readRole); err != nil {
		t.Fatalf("create run-namespace read Role: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), readRole) })
	readBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: readRole.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: readRole.Name},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount,
			Namespace: run.Namespace,
		}},
	}
	if err := c.Create(context.Background(), readBinding); err != nil {
		t.Fatalf("create run-namespace read RoleBinding: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), readBinding) })

	// The identity checkOperatorAccess asks about: the service account above,
	// through impersonation, so the SelfSubjectAccessReviews resolve its real
	// grants.
	impCfg := rest.CopyConfig(envtestServer.Config)
	impCfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + run.Namespace + ":" + serviceAccount,
	}
	impersonated, err := kubernetes.NewForConfig(impCfg)
	if err != nil {
		t.Fatalf("impersonated clientset: %v", err)
	}

	// Once a run-namespace read is allowed, the Role has propagated through
	// the API server's RBAC informers, so the cross-namespace denials below
	// are the scope of the grants and not a sync lag.
	eventually(t, func() error {
		allowed, err := control.subjectAccessReviewAs(context.Background(), serviceAccount,
			accessReview{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name), namespace: run.Namespace})
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("run-namespace pod read not yet allowed")
		}
		return nil
	})

	// The run-namespace reads are allowed: the grants exist where the reads
	// target them.
	for _, review := range []accessReview{
		{group: "", resource: "pods", verb: "get", name: topology.ControlPodName(run.Name), namespace: run.Namespace},
		{group: "", resource: "secrets", verb: "get", name: topology.SigningSecretName(run.Name), namespace: run.Namespace},
		{group: courier.GroupVersion.Group, resource: "laneprofiles", verb: "get", name: run.Spec.Lane, namespace: run.Namespace},
		{group: "", resource: "services", verb: "get", name: topology.BrokerServiceName(run.Name), namespace: run.Namespace},
	} {
		allowed, err := control.subjectAccessReviewAs(context.Background(), serviceAccount, review)
		if err != nil {
			t.Fatalf("run-namespace %s %s review: %v", review.verb, review.resource, err)
		}
		if !allowed {
			t.Errorf("the run-namespace %s %s grant must satisfy the review in namespace %q", review.verb, review.resource, review.namespace)
		}
	}

	// The same secrets-get grant, asked where the credential source lives and
	// where the probe targets live, is refused: a grant scoped to one
	// namespace never authorizes a request in another.
	for _, review := range []accessReview{
		{group: "", resource: "secrets", verb: "get", namespace: operatorNamespace},
		{group: "", resource: "services", verb: "get", name: "kubernetes", namespace: "default"},
	} {
		allowed, err := control.subjectAccessReviewAs(context.Background(), serviceAccount, review)
		if err != nil {
			t.Fatalf("cross-namespace %s %s review: %v", review.verb, review.resource, err)
		}
		if allowed {
			t.Errorf("the run-namespace %s %s grant must not satisfy the review in namespace %q", review.verb, review.resource, review.namespace)
		}
	}

	// The caller's verdict: with the credential-source read unproven, the
	// operator's own check refuses the deployment with the OperatorRBAC
	// verdict the retry gate bounds, not an ordinary error.
	verdict := &SecureControl{Config: SecureConfig{
		RunNamespace:      control.Config.RunNamespace,
		OperatorNamespace: operatorNamespace,
		Kubernetes:        impersonated,
	}}
	err = verdict.checkOperatorAccess(context.Background(), run)
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != "OperatorRBAC" {
		t.Fatalf("checkOperatorAccess error = %v, want an OperatorRBAC verdict", err)
	}
	if !strings.Contains(err.Error(), "secrets") {
		t.Fatalf("checkOperatorAccess error = %v, want the refused secrets read named", err)
	}

	// Grant the reads where they are asked for — the operator namespace for
	// the credential source, default and kube-system for the probe targets —
	// and the same check passes: the denials above were the grant scopes, not
	// the review set itself.
	for _, role := range []*rbacv1.Role{
		{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: run.Name + "-operator-ns-reads"},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: run.Name + "-default-reads"},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"services"}, Verbs: []string{"get"}}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: run.Name + "-kube-system-reads"},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"services"}, Verbs: []string{"get"}}}},
	} {
		if err := c.Create(context.Background(), role); err != nil {
			t.Fatalf("create read Role %s/%s: %v", role.Namespace, role.Name, err)
		}
		binding := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: role.Namespace, Name: role.Name},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name},
			Subjects: []rbacv1.Subject{{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      serviceAccount,
				Namespace: run.Namespace,
			}},
		}
		if err := c.Create(context.Background(), binding); err != nil {
			t.Fatalf("create read RoleBinding %s/%s: %v", role.Namespace, role.Name, err)
		}
		t.Cleanup(func() { _ = c.Delete(context.Background(), role) })
		t.Cleanup(func() { _ = c.Delete(context.Background(), binding) })
	}
	eventually(t, func() error {
		allowed, err := control.subjectAccessReviewAs(context.Background(), serviceAccount,
			accessReview{group: "", resource: "secrets", verb: "get", namespace: operatorNamespace})
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("operator-namespace secrets read not yet allowed")
		}
		return nil
	})
	if err := verdict.checkOperatorAccess(context.Background(), run); err != nil {
		t.Fatalf("checkOperatorAccess with every launch-path grant allowed = %v, want nil", err)
	}
}
