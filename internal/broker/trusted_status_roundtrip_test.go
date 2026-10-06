package broker

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/harness"
)

// TestTrustedStatusEndToEndRoundTrip pins the whole seam together: the
// harness BrokerClient's wire patch, success-status handling, and fencing UID
// must work against the real trusted status handler, not only against
// per-side fakes. The fencing UID it sends must be the one the
// authentication layer produces (the pod's Kubernetes UID).
func TestTrustedStatusEndToEndRoundTrip(t *testing.T) {
	handler, auth, c, identity := trustedHandlerFixture(t, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return nil
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	control := &harness.BrokerClient{BaseURL: server.URL, Token: []byte("trusted-token"), HTTP: server.Client()}

	// A UID-fenced heartbeat and an operation addition, exactly as the
	// control process sends them.
	podUID := string(identity.ControlPodUID)
	if err := control.PostStatus(context.Background(), harness.StatusPatch{
		Heartbeat: &courier.Heartbeat{At: metav1.NewTime(testNow), Kind: "stream", CoordinatorPodUID: podUID},
		AddOperation: &harness.OperationAddition{
			OpID: "shell.b1.aaaa",
			Operation: courier.ActiveOperation{
				BriefID:           "b1",
				CoordinatorPodUID: podUID,
				WorkerPodUID:      "worker-pod-uid",
				DispatchedAt:      metav1.NewTime(testNow),
			},
		},
	}); err != nil {
		t.Fatalf("PostStatus() error = %v; the harness and broker seam must agree on success and wire shape", err)
	}
	if auth.calls != 1 {
		t.Fatalf("authentications = %d, want one live TokenReview per write", auth.calls)
	}
	run := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
		t.Fatal(err)
	}
	if run.Status.Heartbeat == nil || run.Status.Heartbeat.CoordinatorPodUID != podUID {
		t.Fatalf("heartbeat = %#v, want the UID-fenced heartbeat persisted", run.Status.Heartbeat)
	}
	if _, ok := run.Status.ActiveOperations["shell.b1.aaaa"]; !ok {
		t.Fatalf("activeOperations = %#v, want the dispatched operation persisted", run.Status.ActiveOperations)
	}

	// The verified termination clears the entry without dropping the earned
	// heartbeat, and a foreign fencing UID is refused.
	if err := control.PostStatus(context.Background(), harness.StatusPatch{ClearOperation: "shell.b1.aaaa"}); err != nil {
		t.Fatal(err)
	}
	if err := control.PostStatus(context.Background(), harness.StatusPatch{
		Heartbeat: &courier.Heartbeat{At: metav1.NewTime(testNow), Kind: "tool", CoordinatorPodUID: "pod-uid-somewhere-else"},
	}); err == nil {
		t.Fatal("a write fenced to another pod UID must be refused")
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
		t.Fatal(err)
	}
	if len(run.Status.ActiveOperations) != 0 {
		t.Fatalf("activeOperations = %#v, want the verified termination cleared", run.Status.ActiveOperations)
	}
	if run.Status.Heartbeat == nil || run.Status.Heartbeat.Kind != "stream" {
		t.Fatalf("heartbeat = %#v, want the foreign write to have been dropped", run.Status.Heartbeat)
	}
}

var testNow = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
