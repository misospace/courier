package controller

import (
	"context"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
)

// TestCheckBrokerRBACReviewScoping pins the exact access reviews the broker
// check issues, independent of a live authorizer: every pod read is name-scoped
// to a rendered topology pod in the run namespace, the list stays a namespaced
// list, TokenReview is reviewed cluster-scoped (no namespace), and the check
// never issues the unrestricted (unnamed) pod get that failed against the
// name-scoped Role in #644.
func TestCheckBrokerRBACReviewScoping(t *testing.T) {
	const runNamespace = "courier-run"
	run := &courier.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-7", Namespace: runNamespace},
	}

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{courier.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(topology.BrokerRole(run)).Build()

	var reviews []authorizationv1.ResourceAttributes
	clientset := fakek8s.NewSimpleClientset()
	clientset.Fake.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		created := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		reviews = append(reviews, *created.Spec.ResourceAttributes)
		return true, &authorizationv1.SubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
		}, nil
	})

	control := &SecureControl{
		Client: c,
		Config: SecureConfig{RunNamespace: runNamespace, Kubernetes: clientset},
	}
	if err := control.checkBrokerRBAC(context.Background(), run); err != nil {
		t.Fatalf("checkBrokerRBAC with a fully granted broker = %v, want nil", err)
	}

	type want struct{ group, resource, verb, name, namespace string }
	byReview := map[want]int{}
	for _, r := range reviews {
		byReview[want{r.Group, r.Resource, r.Verb, r.Name, r.Namespace}]++
	}

	// Each named topology pod get is issued in the run namespace, and the list
	// too; TokenReview is issued with no namespace.
	for _, name := range []string{
		topology.ControlPodName(run.Name),
		topology.WorkerPodName(run.Name),
		topology.BrokerPodName(run.Name),
	} {
		if got := byReview[want{group: "", resource: "pods", verb: "get", name: name, namespace: runNamespace}]; got != 1 {
			t.Errorf("named pods get %q issued %d times, want once in namespace %q", name, got, runNamespace)
		}
	}
	if got := byReview[want{group: "", resource: "pods", verb: "list", namespace: runNamespace}]; got != 1 {
		t.Errorf("pods list issued %d times, want once in namespace %q", got, runNamespace)
	}
	if got := byReview[want{group: "authentication.k8s.io", resource: "tokenreviews", verb: "create", namespace: ""}]; got != 1 {
		t.Errorf("cluster-scoped tokenreviews create issued %d times, want once with no namespace", got)
	}

	// The old bug: an unrestricted (unnamed, namespaced) pod get the Role never
	// grants. It must never be issued again, under any namespace.
	for _, r := range reviews {
		if r.Resource == "pods" && r.Verb == "get" && r.Name == "" {
			t.Errorf("unrestricted pods get must not be issued (namespace %q)", r.Namespace)
		}
	}
	// TokenReview must never be reviewed under the run namespace.
	for _, r := range reviews {
		if r.Resource == "tokenreviews" && r.Namespace != "" {
			t.Errorf("tokenreviews create reviewed under namespace %q, want cluster-scoped", r.Namespace)
		}
	}
}
