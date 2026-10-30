package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// secureAuthzScheme is the envtest client scheme for the #640 authorization
// suite: core and RBAC objects only, so the suite mirrors exactly the
// cluster-scoped objects the chart provisions.
func secureAuthzScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = rbacv1.AddToScheme(scheme)
	return scheme
}

const (
	authzOperatorNamespace = "courier-native"
	authzOperatorSAName    = "courier-native"
	authzBrokerNamespace   = "courier-runs"
)

// TestSecureOperatorCanBindBrokerTokenReviewRole exercises the real
// API-server RBAC authorizer against the authority the secure chart grants,
// for the per-run broker TokenReview ClusterRoleBinding the operator creates
// during provisioning (#640). envtest's admin client is cluster-admin, so the
// suite impersonates the operator's own ServiceAccount identity to make the
// anti-escalation check run — fake clients and helm rendering never enforce it.
//
// It separates two distinct permissions the production failure conflated:
// the operator's authority to create the ClusterRoleBinding object, and its
// authority to bind a ClusterRole whose permissions it does not itself hold.
func TestSecureOperatorCanBindBrokerTokenReviewRole(t *testing.T) {
	if envtestServer == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	scheme := secureAuthzScheme()
	admin, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	create := func(obj client.Object) {
		t.Helper()
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatalf("admin create %T/%s: %v", obj, obj.GetName(), err)
		}
	}

	create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: authzOperatorNamespace}})
	create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: authzBrokerNamespace}})
	create(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: authzOperatorSAName, Namespace: authzOperatorNamespace,
	}})

	// The broker TokenReview ClusterRole the chart renders, under both the
	// historical default name and a side-by-side release's custom name (#632).
	tokenReviewRules := []rbacv1.PolicyRule{{
		APIGroups: []string{"authentication.k8s.io"}, Resources: []string{"tokenreviews"}, Verbs: []string{"create"},
	}}
	create(brokerClusterRole("courier-broker-tokenreview", tokenReviewRules))
	create(brokerClusterRole("courier-native-broker-tokenreview", tokenReviewRules))
	// An unrelated cluster-scoped role the operator must not be able to bind.
	create(brokerClusterRole("authz-unrelated-role", []rbacv1.PolicyRule{{
		APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"list"},
	}}))
	// An empty role the operator can bind once it may create bindings at all:
	// binding a role that grants nothing escalates nothing.
	create(brokerClusterRole("authz-empty-role", nil))

	// The operator identity, granted the chart's clusterrolebindings create
	// plus the resourceNames-scoped broker bind grant.
	create(operatorClusterRole("authz-operator-role", []string{
		"courier-broker-tokenreview", "courier-native-broker-tokenreview",
	}, true))
	create(bindRoleToSA("authz-operator-binding", "authz-operator-role",
		"system:serviceaccount:"+authzOperatorNamespace+":"+authzOperatorSAName))

	// A second identity granted clusterrolebindings create but NO bind grant:
	// it reproduces the pre-fix 0.1.12 secure operator exactly.
	create(operatorClusterRole("authz-nobind-role", nil, true))
	create(bindRoleToSA("authz-nobind-binding", "authz-nobind-role",
		"system:serviceaccount:"+authzOperatorNamespace+":nobind"))

	operator := impersonate(t, envtestServer.Config, scheme,
		"system:serviceaccount:"+authzOperatorNamespace+":"+authzOperatorSAName)
	nobind := impersonate(t, envtestServer.Config, scheme,
		"system:serviceaccount:"+authzOperatorNamespace+":nobind")

	t.Run("binds broker role under default and custom names", func(t *testing.T) {
		for _, name := range []string{"courier-broker-tokenreview", "courier-native-broker-tokenreview"} {
			binding := brokerBinding("run-"+name, name)
			if err := operator.Create(ctx, binding); err != nil {
				t.Fatalf("operator must bind %q under anti-escalation, got: %v", name, err)
			}
		}
	})

	t.Run("cannot bind an unrelated role", func(t *testing.T) {
		err := operator.Create(ctx, brokerBinding("run-unrelated", "authz-unrelated-role"))
		if err == nil {
			t.Fatal("operator must not bind an unrelated ClusterRole through the scoped grant")
		}
		if !apierrors.IsForbidden(err) {
			t.Fatalf("unrelated bind want Forbidden, got %v", err)
		}
		// The rejection must be the escalation check, not a plain create denial.
		if !strings.Contains(err.Error(), "not currently held") &&
			!strings.Contains(err.Error(), "cannot bind") {
			t.Fatalf("unrelated bind must be refused by anti-escalation, got: %v", err)
		}
	})

	t.Run("binding the object is not authority to bind the role", func(t *testing.T) {
		// The no-bind identity CAN create a binding object when the referenced
		// role grants nothing — so it holds clusterrolebindings create.
		if err := nobind.Create(ctx, brokerBinding("nobind-empty", "authz-empty-role")); err != nil {
			t.Fatalf("no-bind identity should still create a binding to an empty role, got: %v", err)
		}
		// The same identity is refused the broker binding specifically because
		// it may not bind the tokenreviews permission it does not hold.
		err := nobind.Create(ctx, brokerBinding("nobind-broker", "courier-broker-tokenreview"))
		if err == nil {
			t.Fatal("identity without the bind grant must not bind the broker role")
		}
		if !apierrors.IsForbidden(err) {
			t.Fatalf("no-bind broker bind want Forbidden, got %v", err)
		}
		if !strings.Contains(err.Error(), "tokenreviews") {
			t.Fatalf("no-bind refusal must cite the tokenreviews permission it lacks, got: %v", err)
		}
	})
}

func brokerClusterRole(name string, rules []rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules:      rules,
	}
}

// operatorClusterRole mirrors the secure chart's operator authority relevant to
// binding: create/delete/get/list on clusterrolebindings, plus the
// resourceNames-scoped bind grant on the named broker roles when bindNames is
// non-empty.
func operatorClusterRole(name string, bindNames []string, canCreateBindings bool) *rbacv1.ClusterRole {
	rules := []rbacv1.PolicyRule{}
	if canCreateBindings {
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups: []string{rbacv1.GroupName}, Resources: []string{"clusterrolebindings"},
			Verbs: []string{"create", "delete", "get", "list"},
		})
	}
	if len(bindNames) > 0 {
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups: []string{rbacv1.GroupName}, Resources: []string{"clusterroles"},
			Verbs:         []string{"bind"},
			ResourceNames: bindNames,
		})
	}
	return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
}

func bindRoleToSA(bindingName, roleName, saIdentity string) *rbacv1.ClusterRoleBinding {
	parts := strings.Split(strings.TrimPrefix(saIdentity, "system:serviceaccount:"), ":")
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: bindingName},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
		Subjects: []rbacv1.Subject{{
			Kind: "ServiceAccount", Name: parts[1], Namespace: parts[0],
		}},
	}
}

func brokerBinding(name, roleName string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: authzBrokerNamespace + "." + name + "-broker-tokenreview"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
		Subjects: []rbacv1.Subject{{
			Kind: "ServiceAccount", Name: name + "-broker", Namespace: authzBrokerNamespace,
		}},
	}
}

func impersonate(t *testing.T, base *rest.Config, scheme *runtime.Scheme, user string) client.Client {
	t.Helper()
	cfg := rest.CopyConfig(base)
	cfg.Impersonate = rest.ImpersonationConfig{UserName: user}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("impersonate %s: %v", user, err)
	}
	return c
}
