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
		name                 string
		mutateReview         func(*authenticationv1.TokenReview)
		mutateObjects        func(ctrlclient.Client) error
		mutateAfterBootstrap func(ctrlclient.Client) error
		readerAfterBootstrap func(ctrlclient.Reader) ctrlclient.Reader
	}{
		{name: "unauthenticated", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.Authenticated = false }},
		{name: "wrong audience", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.Audiences = []string{"api"} }},
		{name: "missing pod extras", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.User.Extra = nil }},
		{name: "multiple pod uid extras", mutateReview: func(r *authenticationv1.TokenReview) {
			r.Status.User.Extra["authentication.kubernetes.io/pod-uid"] = []string{string(controlPodUID), "other"}
		}},
		{name: "wrong service account uid", mutateReview: func(r *authenticationv1.TokenReview) { r.Status.User.UID = "forged" }},
		{name: "wrong service account name", mutateReview: func(r *authenticationv1.TokenReview) {
			r.Status.User.Username = "system:serviceaccount:" + ns + ":forged"
		}},
		{name: "wrong namespace", mutateReview: func(r *authenticationv1.TokenReview) {
			r.Status.User.Username = "system:serviceaccount:other:" + controlName
		}},
		{name: "missing run label", mutateObjects: mutateControlLabel("courier.misospace.dev/coderrun", "")},
		{name: "missing component label", mutateObjects: mutateControlLabel("courier.misospace.dev/component", "")},
		{name: "wrong component label", mutateObjects: mutateControlLabel("courier.misospace.dev/component", "worker")},
		{name: "terminating control pod", readerAfterBootstrap: func(reader ctrlclient.Reader) ctrlclient.Reader {
			return terminatingPodReader{Reader: reader, podName: controlPod}
		}},

		{name: "forged pod name", mutateReview: func(r *authenticationv1.TokenReview) {
			r.Status.User.Extra["authentication.kubernetes.io/pod-name"] = []string{"forged-control"}
		}},
		{name: "stale pod uid", mutateReview: func(r *authenticationv1.TokenReview) {
			r.Status.User.Extra["authentication.kubernetes.io/pod-uid"] = []string{"old"}
		}},
		{name: "missing live pod", mutateObjects: func(c ctrlclient.Client) error {
			return c.Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: controlPod}})
		}},
		{name: "control pod replaced at same name", mutateObjects: func(c ctrlclient.Client) error {
			p := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: controlPod}, p); err != nil {
				return err
			}
			p.UID = "replacement-control-uid"
			return c.Update(context.Background(), p)
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
			p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "control-new", UID: "new-pod-uid", Labels: controlLabels(), OwnerReferences: []metav1.OwnerReference{{APIVersion: courier.GroupVersion.String(), Kind: "CoderRun", Name: runName, UID: runUID, Controller: boolPtr(true)}}}, Spec: corev1.PodSpec{ServiceAccountName: controlName}}
			return c.Create(context.Background(), p)
		}},
		{name: "unlabeled duplicate live control pod", mutateObjects: func(c ctrlclient.Client) error {
			p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "control-new", UID: "new-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: courier.GroupVersion.String(), Kind: "CoderRun", Name: runName, UID: runUID, Controller: boolPtr(true)}}}, Spec: corev1.PodSpec{ServiceAccountName: controlName}}
			return c.Create(context.Background(), p)
		}},
		{name: "broker pod replaced", mutateAfterBootstrap: func(c ctrlclient.Client) error {
			if err := c.Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: brokerName}}); err != nil {
				return err
			}
			return c.Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: brokerName, UID: "replacement-broker-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: courier.GroupVersion.String(), Kind: "CoderRun", Name: runName, UID: runUID, Controller: boolPtr(true)}}}})
		}},
		{name: "broker pod owner changed", mutateAfterBootstrap: func(c ctrlclient.Client) error {
			p := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: brokerName}, p); err != nil {
				return err
			}
			p.OwnerReferences[0].UID = "other-run"
			return c.Update(context.Background(), p)
		}},
		{name: "run replaced", mutateAfterBootstrap: func(c ctrlclient.Client) error {
			if err := c.Delete(context.Background(), &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: runName}}); err != nil {
				return err
			}
			return c.Create(context.Background(), &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: runName, UID: "replacement-run-uid"}})
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
			if tt.mutateAfterBootstrap != nil {
				if err := tt.mutateAfterBootstrap(client); err != nil {
					t.Fatal(err)
				}
			}
			if tt.readerAfterBootstrap != nil {
				auth.reader = tt.readerAfterBootstrap(auth.reader)
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
	control := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: controlPod, UID: controlPodUID, Labels: controlLabels(), OwnerReferences: []metav1.OwnerReference{owner}}, Spec: corev1.PodSpec{ServiceAccountName: controlName}}
	client := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(run, broker, control).Build()
	tokens := fake.NewSimpleClientset()
	tokens.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) { return true, validReview(), nil })
	return client, tokens
}

type terminatingPodReader struct {
	ctrlclient.Reader
	podName string
}

func (r terminatingPodReader) Get(ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if key.Name == r.podName {
		if pod, ok := obj.(*corev1.Pod); ok {
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		}
	}
	return nil
}

func controlLabels() map[string]string {
	return map[string]string{"courier.misospace.dev/coderrun": runName, "courier.misospace.dev/component": "coordinator"}
}

func mutateControlLabel(key, value string) func(ctrlclient.Client) error {
	return func(c ctrlclient.Client) error {
		p := &corev1.Pod{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: controlPod}, p); err != nil {
			return err
		}
		if value == "" {
			delete(p.Labels, key)
		} else {
			p.Labels[key] = value
		}
		return c.Update(context.Background(), p)
	}
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
