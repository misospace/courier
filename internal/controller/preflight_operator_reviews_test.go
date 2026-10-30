package controller

import (
	"context"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestCheckOperatorAccessReviewScoping pins the namespace each of the
// operator's own access reviews carries.
//
// The operator's provisioning grants are proven before it creates anything, so
// a review that credited a grant the real request would be refused by turns a
// missing cluster grant into a run that launches and then fails. The
// cluster-scoped requests — the broker's ClusterRoleBinding create, the node
// list, the run namespace read, and the two access-review creates — are
// therefore reviewed with no namespace: a namespaced Role can satisfy a
// namespaced review of a cluster-scoped resource, and the RBAC authorizer
// evaluates the attributes literally, so only an empty namespace keeps the
// decision to cluster-wide grants (#650).
func TestCheckOperatorAccessReviewScoping(t *testing.T) {
	const runNamespace = "courier-run"

	var reviews []authorizationv1.ResourceAttributes
	clientset := fakek8s.NewSimpleClientset()
	clientset.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		created := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		reviews = append(reviews, *created.Spec.ResourceAttributes)
		return true, &authorizationv1.SelfSubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
		}, nil
	})

	control := &SecureControl{Config: SecureConfig{RunNamespace: runNamespace, Kubernetes: clientset}}
	if err := control.checkOperatorAccess(context.Background()); err != nil {
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
			t.Errorf("review %s/%s %s in namespace %q issued %d times, want once", k.group, k.resource, k.verb, k.namespace, 1-count)
		}
	}

	// The cluster-scoped resources are never reviewed under the run
	// namespace: that is the whole defect (#650).
	for _, r := range reviews {
		switch r.Resource {
		case "clusterrolebindings", "nodes", "namespaces", "selfsubjectaccessreviews", "subjectaccessreviews":
			if r.Namespace != "" {
				t.Errorf("cluster-scoped %s reviewed under namespace %q, want cluster scope", r.Resource, r.Namespace)
			}
		}
	}
}
