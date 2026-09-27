package broker

import (
	"context"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	courier "github.com/misospace/courier/api/v1alpha1"
)

const (
	ns          = "job"
	runName     = "run-a"
	brokerName  = "broker-a"
	controlName = "control-a"
	controlUID  = "sa-uid"
	controlPod  = "control-a-abc"
)

var (
	runUID        = types.UID("run-uid")
	brokerUID     = types.UID("broker-uid")
	controlPodUID = types.UID("control-pod-uid")
)

func TestAuthenticateLivePodBoundIdentity(t *testing.T) {
	client, tokens := fixture(t)
	auth, err := NewAuthenticator(context.Background(), client, tokens, ns, brokerName, controlName, controlUID)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := auth.Authenticate(context.Background(), "token")
	if err != nil {
		t.Fatal(err)
	}
	if identity.RunUID != runUID || identity.ControlPodUID != controlPodUID || identity.ServiceAccountUID != controlUID {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if got := countCreates(tokens.Actions(), "tokenreviews"); got != 1 {
		t.Fatalf("TokenReview calls = %d; want one", got)
	}
}

func TestAuthenticateRejectsAdversarialReviewOrLiveState(t *testing.T) {
	tests := []struct {
		name          string
		mutateReview  func(*authenticationv1.TokenReview)
		mutateObjects func(ctrlclient.Client) error
	}{
		{name: "unauthenticated", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.Authenticated = false }},
		{name: "wrong audience", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.Audiences = []string{"api"} }},
		{name: "missing pod extras", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.User.Extra = nil }},
		{name: "multiple pod uid extras", mutateReview: func(r *authenticationv1.TokenReview) {
			r.Status.User.Extra["authentication.kubernetes.io/pod-uid"] = []string{string(controlPodUID), "other"}
		}},
		{name: "wrong service account uid", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.User.UID = "forged" }},
		{name: "stale pod uid", mutateReview: func(r *authenticationv1.TokenReview) {
			r.Status.User.Extra["authentication.kubernetes.io/pod-uid"] = []string{"old"}
		}},
		{name: "missing live pod", mutateObjects: func(c ctrlclient.Client) error {
			return c.Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: controlPod}})
		}},
		{name: "wrong pod service account", mutateObjects: func(c ctrlclient.Client) error {
			p := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: controlPod}, p); err != nil {
				return err
			}
			p.Spec.ServiceAccountName = "other"
			return c.Update(context.Background(), p)
		}},
		{name: "foreign owner uid", mutateObjects: func(c ctrlclient.Client) error {
			p := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: controlPod}, p); err != nil {
				return err
			}
			p.OwnerReferences[0].UID = "other-run"
			return c.Update(context.Background(), p)
		}},
		{name: "second live control incarnation", mutateObjects: func(c ctrlclient.Client) error {
			p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "control-new", UID: "new-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: courier.GroupVersion.String(), Kind: "CoderRun", Name: runName, UID: runUID, Controller: boolPtr(true)}}}, Spec: corev1.PodSpec{ServiceAccountName: controlName}}
			return c.Create(context.Background(), p)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, tokens := fixture(t)
			if tt.mutateObjects != nil {
				if err := tt.mutateObjects(client); err != nil {
					t.Fatal(err)
				}
			}
			if tt.mutateReview != nil {
				tokens.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
					review := validReview()
					tt.mutateReview(review)
					return true, review, nil
				})
			}
			auth, err := NewAuthenticator(context.Background(), client, tokens, ns, brokerName, controlName, controlUID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = auth.Authenticate(context.Background(), "token"); err == nil {
				t.Fatal("Authenticate succeeded; want rejection")
			}
		})
	}
}

func TestBootstrapRejectsMismatchedRunOwnerUID(t *testing.T) {
	client, tokens := fixture(t)
	pod := &corev1.Pod{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: brokerName}, pod); err != nil {
		t.Fatal(err)
	}
	pod.OwnerReferences[0].UID = "old-run-uid"
	if err := client.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAuthenticator(context.Background(), client, tokens, ns, brokerName, controlName, controlUID); err == nil {
		t.Fatal("bootstrap accepted stale owner UID")
	}
}

func TestAuthenticateRejectsReviewFailureAndMissingToken(t *testing.T) {
	client, tokens := fixture(t)
	auth, err := NewAuthenticator(context.Background(), client, tokens, ns, brokerName, controlName, controlUID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Authenticate(context.Background(), ""); err == nil {
		t.Fatal("empty token accepted")
	}
	tokens.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, context.DeadlineExceeded })
	if _, err := auth.Authenticate(context.Background(), "token"); err == nil {
		t.Fatal("TokenReview error accepted")
	}
}

func fixture(t *testing.T) (ctrlclient.Client, *fake.Clientset) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = courier.AddToScheme(scheme)
	run := &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: runName, UID: runUID}}
	owner := metav1.OwnerReference{APIVersion: courier.GroupVersion.String(), Kind: "CoderRun", Name: runName, UID: runUID, Controller: boolPtr(true)}
	broker := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: brokerName, UID: brokerUID, OwnerReferences: []metav1.OwnerReference{owner}}}
	control := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: controlPod, UID: controlPodUID, OwnerReferences: []metav1.OwnerReference{owner}}, Spec: corev1.PodSpec{ServiceAccountName: controlName}}
	client := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(run, broker, control).Build()
	tokens := fake.NewSimpleClientset()
	tokens.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) { return true, validReview(), nil })
	return client, tokens
}

func validReview() *authenticationv1.TokenReview {
	return &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{Authenticated: true, Audiences: []string{BrokerAudience}, User: authenticationv1.UserInfo{Username: "system:serviceaccount:" + ns + ":" + controlName, UID: controlUID, Extra: map[string]authenticationv1.ExtraValue{"authentication.kubernetes.io/pod-name": {controlPod}, "authentication.kubernetes.io/pod-uid": {string(controlPodUID)}}}}}
}
func boolPtr(v bool) *bool { return &v }
func countCreates(actions []ktesting.Action, resource string) int {
	n := 0
	for _, a := range actions {
		if a.GetVerb() == "create" && a.GetResource().Resource == resource {
			n++
		}
	}
	return n
}
