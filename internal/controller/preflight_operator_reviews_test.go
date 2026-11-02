package controller

import (
	"context"
	"strings"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
)

// TestCheckOperatorAccessReviewScoping pins the namespace and name each of
// the operator's own access reviews carries.
//
// The operator's grants are proven before it creates anything, so a review
// that credited a grant the real request would be refused by either turns a
// missing cluster grant into a run that launches and then fails, or hides a
// missing namespaced grant behind a namespace the request does not target.
// Each review therefore mirrors the request it stands for: the cluster-scoped
// requests — the broker's ClusterRoleBinding create and delete, the node
// list, the run namespace read, and the two access-review creates — carry no
// namespace, so a namespaced Role cannot satisfy them (#650), and every
// namespaced request is reviewed in the namespace it targets, including the
// reads the secure launch path makes outside the run namespace (#655).
func TestCheckOperatorAccessReviewScoping(t *testing.T) {
	const (
		runNamespace      = "courier-run"
		operatorNamespace = "courier-system"
	)

	// The proof follows the real request path: per-run skill ConfigMap reads
	// and writes exist only for a run with persisted pins, so those reviews
	// appear only for the run whose real requests will name them (#700,
	// #701). The teardown deletes and the bundle config ConfigMap delete are
	// unconditional: Revoke issues them for every run, skillless included
	// (#703).
	for _, tc := range []struct {
		name string
		pins []courier.SkillPin
	}{
		{name: "skillless"},
		{name: "pinned", pins: []courier.SkillPin{
			{Name: "simple-english", Digest: "sha256:" + strings.Repeat("a", 64)},
			{Name: "review-pass", Digest: "sha256:" + strings.Repeat("b", 64)},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reviews []authorizationv1.ResourceAttributes
			clientset := fakek8s.NewSimpleClientset()
			clientset.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
				created := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
				reviews = append(reviews, *created.Spec.ResourceAttributes)
				return true, &authorizationv1.SelfSubjectAccessReview{
					Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
				}, nil
			})

			run := secureRun("review-scoping")
			run.Status.PublicationPolicy = &courier.PublicationPolicy{
				RunUID:              string(run.UID),
				ProviderConfigRef:   "github",
				CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
			}
			run.Status.SkillPins = tc.pins
			control := &SecureControl{Config: SecureConfig{
				RunNamespace:      runNamespace,
				OperatorNamespace: operatorNamespace,
				Registry:          secureRegistry(),
				Kubernetes:        clientset,
			}}
			if err := control.checkOperatorAccess(context.Background(), run); err != nil {
				t.Fatalf("checkOperatorAccess with every grant allowed = %v, want nil", err)
			}

			type key struct{ group, resource, verb, name, namespace string }
			want := map[key]int{
				{"", "secrets", "create", "", runNamespace}:                               1,
				{"", "secrets", "delete", "", runNamespace}:                               1,
				{"", "serviceaccounts", "create", "", runNamespace}:                       1,
				{"", "serviceaccounts", "delete", "", runNamespace}:                       1,
				{"", "pods", "create", "", runNamespace}:                                  1,
				{"", "pods", "delete", "", runNamespace}:                                  1,
				{"", "services", "create", "", runNamespace}:                              1,
				{"", "services", "delete", "", runNamespace}:                              1,
				{"networking.k8s.io", "networkpolicies", "create", "", runNamespace}:      1,
				{"networking.k8s.io", "networkpolicies", "delete", "", runNamespace}:      1,
				{"rbac.authorization.k8s.io", "roles", "create", "", runNamespace}:        1,
				{"rbac.authorization.k8s.io", "rolebindings", "create", "", runNamespace}: 1,
				{"rbac.authorization.k8s.io", "clusterrolebindings", "create", "", ""}:    1,
				{"", "nodes", "list", "", ""}:                                             1,
				{"", "namespaces", "get", "", ""}:                                         1,
				{"authorization.k8s.io", "selfsubjectaccessreviews", "create", "", ""}:    1,
				{"authorization.k8s.io", "subjectaccessreviews", "create", "", ""}:        1,
				// The launch-path reads (#655): each mirrors a real request at the
				// scope that request has. topologyPodsExist finds the pods through a
				// cached run-namespace list — the only pod read on the path — so the
				// list is what the review mirrors, and no named gets are manufactured.
				{"", "pods", "list", "", runNamespace}:                                           1,
				{courier.GroupVersion.Group, "laneprofiles", "get", run.Spec.Lane, runNamespace}: 1,
				{"", "secrets", "get", topology.SigningSecretName(run.Name), runNamespace}:       1,
				// The credential source Secrets the selected registration names, read
				// in the operator's own namespace: the reviews carry the names the
				// real requests will carry, so a resourceNames-scoped Role granting
				// exactly those Secrets satisfies the proof.
				{"", "secrets", "get", "forge-creds", operatorNamespace}:                    1,
				{"", "services", "get", "kubernetes", "default"}:                            1,
				{"", "services", "get", "kube-dns", "kube-system"}:                          1,
				{"", "services", "get", topology.BrokerServiceName(run.Name), runNamespace}: 1,
				// Teardown deletes (#703): Revoke removes the run's broker
				// Role, RoleBinding, and token-review ClusterRoleBinding by
				// name on every run, whatever it pinned, so the proof
				// carries those deletes too — names from the same render
				// calls Revoke deletes.
				{"rbac.authorization.k8s.io", "roles", "delete", topology.BrokerRole(run).Name, runNamespace}:                                                    1,
				{"rbac.authorization.k8s.io", "rolebindings", "delete", topology.BrokerRoleBinding(run).Name, runNamespace}:                                      1,
				{"rbac.authorization.k8s.io", "clusterrolebindings", "delete", topology.BrokerTokenReviewBinding(run, topology.TokenReviewClusterRole).Name, ""}: 1,
			}
			// The skill ConfigMap reads and writes (#700, #701): the
			// materialization reads every pinned bundle's content
			// ConfigMap and the bundle config ConfigMap by name in the
			// run namespace, creates the ones that are missing, and
			// deletes and recreates one whose content drifted, so the
			// reviews carry get, create, and delete for the content
			// names and get and create for the bundle config name; a
			// skillless run performs no such request and earns no
			// materialization review. The bundle config ConfigMap's
			// delete is Revoke's teardown write, issued for every run
			// (#703).
			var skillConfigMaps []string
			for _, pin := range tc.pins {
				skillConfigMaps = append(skillConfigMaps, topology.SkillContentConfigMapName(run.Name, pin.Name))
			}
			for _, name := range skillConfigMaps {
				for _, verb := range []string{"get", "create", "delete"} {
					want[key{"", "configmaps", verb, name, runNamespace}] = 1
				}
			}
			bundlesConfigMap := topology.SkillBundlesConfigMapName(run.Name)
			want[key{"", "configmaps", "delete", bundlesConfigMap, runNamespace}] = 1
			if len(tc.pins) > 0 {
				want[key{"", "configmaps", "get", bundlesConfigMap, runNamespace}] = 1
				want[key{"", "configmaps", "create", bundlesConfigMap, runNamespace}] = 1
			}

			for _, r := range reviews {
				k := key{r.Group, r.Resource, r.Verb, r.Name, r.Namespace}
				count, ok := want[k]
				if !ok {
					t.Errorf("unexpected review %s/%s %s (name %q) in namespace %q", r.Group, r.Resource, r.Verb, r.Name, r.Namespace)
					continue
				}
				if count == 0 {
					t.Errorf("review %s/%s %s in namespace %q issued more than once", r.Group, r.Resource, r.Verb, r.Namespace)
					continue
				}
				want[k] = count - 1
			}
			for k, count := range want {
				if count != 0 {
					t.Errorf("review %s/%s %s (name %q) in namespace %q issued %d times, want once", k.group, k.resource, k.verb, k.name, k.namespace, 1-count)
				}
			}

			// The cluster-scoped resources are never reviewed under a namespace: that
			// is the whole defect (#650).
			for _, r := range reviews {
				switch r.Resource {
				case "clusterrolebindings", "nodes", "namespaces", "selfsubjectaccessreviews", "subjectaccessreviews":
					if r.Namespace != "" {
						t.Errorf("cluster-scoped %s reviewed under namespace %q, want cluster scope", r.Resource, r.Namespace)
					}
				}
			}
		})
	}
}

// TestRevokeDeletesAreProvenByTheReviewSet cross-checks the proof against the
// teardown path: every object Revoke deletes must be covered by a delete the
// access-review set carries — same group, resource, and namespace, with the
// review naming the same object or no object at all, because an unnamed
// review stands for an unrestricted grant that admits every named request
// too.
//
// The pinned review-set test snapshots the set; this test catches the other
// direction of drift, the one #703 found: a delete Revoke grows (or already
// issues) that no review promises, which passes the preflight and then keeps
// a deleted run's finalizer stuck on a Forbidden the teardown treats as a
// hard error.
func TestRevokeDeletesAreProvenByTheReviewSet(t *testing.T) {
	for _, tc := range []struct {
		name string
		pins []courier.SkillPin
	}{
		{name: "skillless"},
		{name: "pinned", pins: []courier.SkillPin{
			{Name: "simple-english", Digest: "sha256:" + strings.Repeat("a", 64)},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{
				courier.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, networkingv1.AddToScheme,
			} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			var deleted []ctrlclient.Object
			c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(ctx context.Context, c ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.DeleteOption) error {
					deleted = append(deleted, obj.DeepCopyObject().(ctrlclient.Object))
					return c.Delete(ctx, obj, opts...)
				},
			}).Build()
			run := secureRun("revoke-proven")
			run.Status.SkillPins = tc.pins
			control := &SecureControl{Client: c, Config: SecureConfig{
				RunNamespace:      secureTestNamespace,
				OperatorNamespace: "courier-system",
			}}
			// An empty cluster: no pods or broker Service to wait out, so one
			// pass reaches the material teardown and every delete it issues is
			// refused only by absence, which Revoke treats as done.
			done, err := control.Revoke(context.Background(), run)
			if err != nil || !done {
				t.Fatalf("Revoke on an empty topology = done=%v err=%v, want done", done, err)
			}
			if len(deleted) == 0 {
				t.Fatal("Revoke issued no deletes, so the cross-check proves nothing")
			}
			registration := secureRegistry().Registrations()[0]
			provesDelete := func(group, resource, name, namespace string) bool {
				for _, review := range control.operatorAccessReviews(run, &registration) {
					if review.verb != "delete" || review.group != group || review.resource != resource || review.namespace != namespace {
						continue
					}
					if review.name != "" && review.name != name {
						continue
					}
					return true
				}
				return false
			}
			for _, obj := range deleted {
				group, resource := revokeObjectGroupResource(obj)
				if resource == "" {
					t.Errorf("Revoke deleted unclassified object %T; classify it in revokeObjectGroupResource", obj)
					continue
				}
				if !provesDelete(group, resource, obj.GetName(), obj.GetNamespace()) {
					t.Errorf("Revoke deletes %s %s/%s but the access-review set carries no covering delete",
						resource, obj.GetNamespace(), obj.GetName())
				}
			}
		})
	}
}

// revokeObjectGroupResource maps the object types Revoke deletes to the
// group and plural resource the access reviews ask them under. An
// unclassified type reports an empty resource so the test fails closed.
func revokeObjectGroupResource(obj ctrlclient.Object) (string, string) {
	switch obj.(type) {
	case *corev1.Secret:
		return "", "secrets"
	case *corev1.ConfigMap:
		return "", "configmaps"
	case *corev1.Service:
		return "", "services"
	case *corev1.ServiceAccount:
		return "", "serviceaccounts"
	case *corev1.Pod:
		return "", "pods"
	case *rbacv1.Role:
		return "rbac.authorization.k8s.io", "roles"
	case *rbacv1.RoleBinding:
		return "rbac.authorization.k8s.io", "rolebindings"
	case *rbacv1.ClusterRoleBinding:
		return "rbac.authorization.k8s.io", "clusterrolebindings"
	case *networkingv1.NetworkPolicy:
		return "networking.k8s.io", "networkpolicies"
	}
	return "", ""
}
