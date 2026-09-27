package broker

import (
	"context"
	"fmt"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
)

const BrokerAudience = "courier-broker"

// Identity is derived exclusively from a successful TokenReview and live API reads.
type Identity struct {
	RunUID            types.UID
	RunName           string
	Namespace         string
	ControlPod        string
	ControlPodUID     types.UID
	ServiceAccount    string
	ServiceAccountUID string
}

// Authenticator verifies pod-bound control tokens against live Kubernetes state.
// The supplied controller-runtime Reader must read live API state, not a manager
// cache. This cannot be proven from the Reader interface; callers must provide
// an uncached client (and tests use a direct fake client).
type Authenticator struct {
	reader        ctrlclient.Reader
	tokens        kubernetes.Interface
	namespace     string
	brokerPodName string
	brokerPodUID  types.UID
	runName       string
	runUID        types.UID
	controlSAName string
	controlSAUID  string
}

// NewAuthenticator bootstraps immutable run identity by following the configured
// broker pod's live controller owner reference. It fails closed on any ambiguity.
func NewAuthenticator(ctx context.Context, reader ctrlclient.Reader, tokens kubernetes.Interface, namespace, brokerPodName, controlSAName, controlSAUID string) (*Authenticator, error) {
	if reader == nil || tokens == nil || namespace == "" || brokerPodName == "" || controlSAName == "" || controlSAUID == "" {
		return nil, fmt.Errorf("broker authentication configuration is incomplete")
	}
	pod := &corev1.Pod{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: brokerPodName}, pod); err != nil {
		return nil, fmt.Errorf("read broker pod: %w", err)
	}
	if pod.DeletionTimestamp != nil || pod.UID == "" {
		return nil, fmt.Errorf("broker pod is terminating or has no UID")
	}
	runName, runUID, ok := controllerRunOwner(pod)
	if !ok {
		return nil, fmt.Errorf("broker pod has no unambiguous CoderRun controller owner")
	}
	run := &courier.CoderRun{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: runName}, run); err != nil {
		return nil, fmt.Errorf("read broker owner CoderRun: %w", err)
	}
	if run.UID == "" || run.UID != runUID || run.DeletionTimestamp != nil {
		return nil, fmt.Errorf("broker pod owner does not identify a live CoderRun incarnation")
	}
	return &Authenticator{reader: reader, tokens: tokens, namespace: namespace, brokerPodName: brokerPodName, brokerPodUID: pod.UID, runName: runName, runUID: runUID, controlSAName: controlSAName, controlSAUID: controlSAUID}, nil
}

// Authenticate validates one bearer token. Call for every control request; no
// successful result is cached because pod incarnation is part of authorization.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (Identity, error) {
	if token == "" || strings.TrimSpace(token) != token {
		return Identity{}, fmt.Errorf("missing or malformed bearer token")
	}
	tr, err := a.tokens.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: token, Audiences: []string{BrokerAudience}}}, metav1.CreateOptions{})
	if err != nil {
		return Identity{}, fmt.Errorf("review control token: %w", err)
	}
	status := tr.Status
	if !status.Authenticated || !contains(status.Audiences, BrokerAudience) {
		return Identity{}, fmt.Errorf("token is unauthenticated or lacks broker audience")
	}
	user := status.User
	prefix := "system:serviceaccount:" + a.namespace + ":"
	if !strings.HasPrefix(user.Username, prefix) || strings.TrimPrefix(user.Username, prefix) != a.controlSAName || user.UID != a.controlSAUID {
		return Identity{}, fmt.Errorf("token subject is not the configured control service account")
	}
	podName, okName := singleExtra(user.Extra, "authentication.kubernetes.io/pod-name")
	podUID, okUID := singleExtra(user.Extra, "authentication.kubernetes.io/pod-uid")
	if !okName || !okUID || podName == "" || podUID == "" {
		return Identity{}, fmt.Errorf("token review lacks pod-bound identity")
	}
	pod := &corev1.Pod{}
	if err := a.reader.Get(ctx, types.NamespacedName{Namespace: a.namespace, Name: podName}, pod); err != nil {
		return Identity{}, fmt.Errorf("read authenticated control pod: %w", err)
	}
	if pod.DeletionTimestamp != nil || string(pod.UID) != podUID || pod.Spec.ServiceAccountName != a.controlSAName || pod.Labels["courier.misospace.dev/coderrun"] != a.runName || pod.Labels["courier.misospace.dev/component"] != "coordinator" {
		return Identity{}, fmt.Errorf("authenticated control pod is stale, terminating, or uses another service account")
	}
	runName, runUID, ok := controllerRunOwner(pod)
	if !ok || runName != a.runName || runUID != a.runUID {
		return Identity{}, fmt.Errorf("control pod is not owned by this broker's CoderRun")
	}
	run := &courier.CoderRun{}
	if err := a.reader.Get(ctx, types.NamespacedName{Namespace: a.namespace, Name: a.runName}, run); err != nil {
		return Identity{}, fmt.Errorf("read current CoderRun: %w", err)
	}
	if run.UID != a.runUID || run.DeletionTimestamp != nil {
		return Identity{}, fmt.Errorf("broker CoderRun incarnation is no longer live")
	}
	// A run may have only one live control incarnation. Scan the namespace and
	// classify by owner UID + service account, not labels: labels are mutable and
	// an unlabeled duplicate must still make authorization ambiguous.
	pods := &corev1.PodList{}
	if err := a.reader.List(ctx, pods, ctrlclient.InNamespace(a.namespace)); err != nil {
		return Identity{}, fmt.Errorf("list current run pods: %w", err)
	}
	liveControls := 0
	for i := range pods.Items {
		candidate := &pods.Items[i]
		ownerName, ownerUID, owned := controllerRunOwner(candidate)
		if owned && ownerName == a.runName && ownerUID == a.runUID && candidate.Spec.ServiceAccountName == a.controlSAName && candidate.DeletionTimestamp == nil {
			liveControls++
			if candidate.Name != podName || candidate.UID != pod.UID {
				return Identity{}, fmt.Errorf("another live control pod incarnation exists")
			}
		}
	}
	if liveControls != 1 {
		return Identity{}, fmt.Errorf("expected exactly one live control pod incarnation")
	}
	// Re-read our own pod too: a replacement must not inherit this broker's authority.
	broker := &corev1.Pod{}
	if err := a.reader.Get(ctx, types.NamespacedName{Namespace: a.namespace, Name: a.brokerPodName}, broker); err != nil {
		return Identity{}, fmt.Errorf("read current broker pod: %w", err)
	}
	brokerRunName, brokerRunUID, brokerOwned := controllerRunOwner(broker)
	if broker.UID != a.brokerPodUID || broker.DeletionTimestamp != nil || !brokerOwned || brokerRunName != a.runName || brokerRunUID != a.runUID {
		return Identity{}, fmt.Errorf("broker pod incarnation or owner is no longer live")
	}
	return Identity{RunUID: a.runUID, RunName: a.runName, Namespace: a.namespace, ControlPod: podName, ControlPodUID: pod.UID, ServiceAccount: a.controlSAName, ServiceAccountUID: user.UID}, nil
}

func controllerRunOwner(pod *corev1.Pod) (string, types.UID, bool) {
	var name string
	var uid types.UID
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.APIVersion != courier.GroupVersion.String() || owner.Kind != "CoderRun" || owner.Name == "" || owner.UID == "" || name != "" {
			return "", "", false
		}
		name, uid = owner.Name, owner.UID
	}
	return name, uid, name != "" && uid != ""
}

func singleExtra(extra map[string]authenticationv1.ExtraValue, key string) (string, bool) {
	values := extra[key]
	if len(values) != 1 {
		return "", false
	}
	return values[0], values[0] != ""
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
