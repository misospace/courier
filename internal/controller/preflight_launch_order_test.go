package controller

import (
	"context"
	"errors"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// TestLaunchSecureProvesOperatorGrantsBeforeProvisioning pins the launch
// path's ordering: the deployment's own facts are proven before the operator
// creates anything.
//
// A missing operator grant is a deployment misconfiguration the operator must
// fix, and the verdict that says so is what lets the caller bound the retry.
// Had the topology been created first, the same misconfiguration would surface
// as a raw Forbidden from a create — an ordinary error, which the design
// classifies as transient and retries as fast as the API server allows, which
// is the Pending → Claimed → Pending churn #644 requires to stop.
func TestLaunchSecureProvesOperatorGrantsBeforeProvisioning(t *testing.T) {
	control, c := secureControl(t)
	denied := fakek8s.NewSimpleClientset()
	denied.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SelfSubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{Allowed: false},
		}, nil
	})
	control.Config.Kubernetes = denied

	run := secureRun("operator-grants")
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:              string(run.UID),
		ProviderConfigRef:   "github",
		ProviderEndpoint:    "https://api.github.com/",
		CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
		BaseRepo:            "Acme/Widgets",
		BaseRef:             "main",
		BaseOID:             "baseoid",
		WorkRepo:            "Acme/Widgets",
		WorkRef:             "courier/Acme/Widgets/issue-7",
		WorkInitiallyAbsent: true,
	}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   secureTestNamespace,
			Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
		},
	}
	if err := c.Create(context.Background(), namespace); err != nil {
		t.Fatalf("create run namespace: %v", err)
	}
	// The credential source the per-run copy is made from, so provisioning
	// would succeed if it ran: the only reason nothing is created below is
	// that the deployment's own facts are proven first.
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("tok-123")},
	}
	if err := c.Create(context.Background(), creds); err != nil {
		t.Fatalf("create credential source: %v", err)
	}

	_, err := control.LaunchSecure(context.Background(), run)
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != "OperatorRBAC" {
		t.Fatalf("LaunchSecure error = %v, want an OperatorRBAC verdict", err)
	}

	// Nothing was created: the deployment's own facts are proven before the
	// operator creates anything, so a missing grant costs no topology.
	serviceAccounts := &corev1.ServiceAccountList{}
	if err := c.List(context.Background(), serviceAccounts, ctrlclient.InNamespace(secureTestNamespace)); err != nil {
		t.Fatalf("list service accounts: %v", err)
	}
	if len(serviceAccounts.Items) != 0 {
		t.Fatalf("service accounts = %#v, want the topology left unprovisioned", serviceAccounts.Items)
	}
}

// TestCheckOperatorAccessForbiddenIsAVerdict pins the last hole in the
// operator-grant proof: the operator needs a grant to ask the access-review
// question at all, and a deployment that lacks it cannot be told apart from a
// transient failure by the error alone. A refusal is a deployment
// misconfiguration the operator must fix, so it must arrive as the verdict the
// retry gate bounds rather than as an ordinary error the design retries as fast
// as the API server allowed.
func TestCheckOperatorAccessForbiddenIsAVerdict(t *testing.T) {
	control, _ := secureControl(t)
	forbidden := fakek8s.NewSimpleClientset()
	forbidden.Fake.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "authorization.k8s.io", Resource: "selfsubjectaccessreviews"},
			"", errors.New("no grant"))
	})
	control.Config.Kubernetes = forbidden

	err := control.checkOperatorAccess(context.Background())
	var permanent *SecureNeedsHumanError
	if !errors.As(err, &permanent) || permanent.Reason != "OperatorRBAC" {
		t.Fatalf("checkOperatorAccess error = %v, want an OperatorRBAC verdict", err)
	}
}
