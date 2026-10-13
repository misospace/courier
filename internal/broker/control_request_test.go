package broker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
)

func setLiveControlRequest(t *testing.T, c client.Client, request *courier.ControlRequest) {
	t.Helper()
	run := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.ControlRequest = request
	if err := c.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
}

func postControlRequestAck(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, PathTrustedStatus, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer trusted-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func getControlRequest(t *testing.T, handler http.Handler, target string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, body)
	req.Header.Set("Authorization", "Bearer trusted-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestTrustedControlRequestAckAccepted pins the durable acknowledgement path:
// an ack fenced to the live request the authenticated incarnation is targeted
// by is persisted through the trusted status write.
func TestTrustedControlRequestAckAccepted(t *testing.T) {
	handler, _, c, identity := trustedHandlerFixture(t, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil })
	setLiveControlRequest(t, c, &courier.ControlRequest{
		ID: "softstop-1", Kind: courier.ControlRequestSoftStop,
		TargetPodUID: string(identity.ControlPodUID), RequestedAt: metav1.NewTime(testNow),
	})
	rec := postControlRequestAck(t, handler, `{"controlRequestAck":{"id":"softstop-1","at":"2026-10-05T12:00:00Z"}}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %q; want 204", rec.Code, rec.Body.String())
	}
	run := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
		t.Fatal(err)
	}
	wantAt := metav1.NewTime(testNow)
	if run.Status.ControlRequestAck == nil || run.Status.ControlRequestAck.ID != "softstop-1" || !run.Status.ControlRequestAck.At.Equal(&wantAt) {
		t.Fatalf("controlRequestAck = %#v, want the persisted acknowledgement", run.Status.ControlRequestAck)
	}
	if run.Status.ControlRequest == nil || run.Status.ControlRequest.ID != "softstop-1" {
		t.Fatalf("controlRequest = %#v, want the live request preserved", run.Status.ControlRequest)
	}
}

// TestTrustedControlRequestAckRefused pins the fence: an acknowledgement is
// served only when the live request names the same ID and fences to the
// authenticated incarnation.
func TestTrustedControlRequestAckRefused(t *testing.T) {
	validate := func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil }
	t.Run("no-live-request", func(t *testing.T) {
		handler, _, c, _ := trustedHandlerFixture(t, validate)
		rec := postControlRequestAck(t, handler, `{"controlRequestAck":{"id":"softstop-1","at":"2026-10-05T12:00:00Z"}}`)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, body = %q; want 422", rec.Code, rec.Body.String())
		}
		run := &courier.CoderRun{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
			t.Fatal(err)
		}
		if run.Status.ControlRequestAck != nil {
			t.Fatalf("refused ack persisted: %#v", run.Status.ControlRequestAck)
		}
	})
	t.Run("id-mismatch", func(t *testing.T) {
		handler, _, c, identity := trustedHandlerFixture(t, validate)
		setLiveControlRequest(t, c, &courier.ControlRequest{
			ID: "softstop-1", Kind: courier.ControlRequestSoftStop,
			TargetPodUID: string(identity.ControlPodUID), RequestedAt: metav1.NewTime(testNow),
		})
		rec := postControlRequestAck(t, handler, `{"controlRequestAck":{"id":"softstop-2","at":"2026-10-05T12:00:00Z"}}`)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, body = %q; want 422", rec.Code, rec.Body.String())
		}
		run := &courier.CoderRun{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
			t.Fatal(err)
		}
		if run.Status.ControlRequestAck != nil {
			t.Fatalf("mismatched ack persisted: %#v", run.Status.ControlRequestAck)
		}
	})
	t.Run("foreign-target-uid", func(t *testing.T) {
		handler, _, c, _ := trustedHandlerFixture(t, validate)
		// The live request fences to a previous, dead incarnation: the
		// authenticated live pod may not acknowledge it.
		setLiveControlRequest(t, c, &courier.ControlRequest{
			ID: "softstop-1", Kind: courier.ControlRequestSoftStop,
			TargetPodUID: "pod-uid-previous", RequestedAt: metav1.NewTime(testNow),
		})
		rec := postControlRequestAck(t, handler, `{"controlRequestAck":{"id":"softstop-1","at":"2026-10-05T12:00:00Z"}}`)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, body = %q; want 422", rec.Code, rec.Body.String())
		}
		run := &courier.CoderRun{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
			t.Fatal(err)
		}
		if run.Status.ControlRequestAck != nil {
			t.Fatalf("foreign-target ack persisted: %#v", run.Status.ControlRequestAck)
		}
	})
	t.Run("empty-id", func(t *testing.T) {
		handler, _, c, identity := trustedHandlerFixture(t, validate)
		setLiveControlRequest(t, c, &courier.ControlRequest{
			ID: "softstop-1", Kind: courier.ControlRequestSoftStop,
			TargetPodUID: string(identity.ControlPodUID), RequestedAt: metav1.NewTime(testNow),
		})
		rec := postControlRequestAck(t, handler, `{"controlRequestAck":{"id":"","at":"2026-10-05T12:00:00Z"}}`)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, body = %q; want 422", rec.Code, rec.Body.String())
		}
		run := &courier.CoderRun{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
			t.Fatal(err)
		}
		if run.Status.ControlRequestAck != nil {
			t.Fatalf("empty-id ack persisted: %#v", run.Status.ControlRequestAck)
		}
	})
	t.Run("missing-at", func(t *testing.T) {
		handler, _, c, identity := trustedHandlerFixture(t, validate)
		setLiveControlRequest(t, c, &courier.ControlRequest{
			ID: "softstop-1", Kind: courier.ControlRequestSoftStop,
			TargetPodUID: string(identity.ControlPodUID), RequestedAt: metav1.NewTime(testNow),
		})
		rec := postControlRequestAck(t, handler, `{"controlRequestAck":{"id":"softstop-1"}}`)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, body = %q; want a definite 422, never a retryable category", rec.Code, rec.Body.String())
		}
		run := &courier.CoderRun{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
			t.Fatal(err)
		}
		if run.Status.ControlRequestAck != nil {
			t.Fatalf("missing-at ack persisted: %#v", run.Status.ControlRequestAck)
		}
	})
}

// TestTrustedControlRequestRead pins the authenticated read of the operator's
// fenced control request on the trusted listener.
func TestTrustedControlRequestRead(t *testing.T) {
	validate := func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil }
	t.Run("exact-record", func(t *testing.T) {
		handler, auth, c, identity := trustedHandlerFixture(t, validate)
		setLiveControlRequest(t, c, &courier.ControlRequest{
			ID: "softstop-1", Kind: courier.ControlRequestSoftStop,
			TargetPodUID: string(identity.ControlPodUID), RequestedAt: metav1.NewTime(testNow),
		})
		rec := getControlRequest(t, handler, PathControlRequest, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %q; want 200", rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("content-type = %q, want application/json", got)
		}
		want := `{"id":"softstop-1","kind":"soft-stop","targetPodUID":"pod-uid-a","requestedAt":"2026-10-05T12:00:00Z"}
`
		if rec.Body.String() != want {
			t.Fatalf("body = %q, want the record verbatim %q", rec.Body.String(), want)
		}
		if auth.calls != 1 {
			t.Fatalf("authentication calls = %d, want one live authentication", auth.calls)
		}
	})
	t.Run("no-request", func(t *testing.T) {
		handler, _, _, _ := trustedHandlerFixture(t, validate)
		rec := getControlRequest(t, handler, PathControlRequest, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, body = %q; want 404", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "softstop") {
			t.Fatalf("404 body reveals a request: %q", rec.Body.String())
		}
	})
	t.Run("foreign-target", func(t *testing.T) {
		handler, _, c, _ := trustedHandlerFixture(t, validate)
		setLiveControlRequest(t, c, &courier.ControlRequest{
			ID: "softstop-1", Kind: courier.ControlRequestSoftStop,
			TargetPodUID: "pod-uid-previous", RequestedAt: metav1.NewTime(testNow),
		})
		rec := getControlRequest(t, handler, PathControlRequest, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, body = %q; want 404", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "softstop-1") {
			t.Fatalf("404 body reveals the record: %q", rec.Body.String())
		}
	})
	t.Run("non-canonical-spelling", func(t *testing.T) {
		handler, auth, _, _ := trustedHandlerFixture(t, validate)
		for _, target := range []string{PathControlRequest + "/", PathControlRequest + "?x=1", "/trusted/v1//control-request"} {
			rec := getControlRequest(t, handler, target, nil)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("target %q status = %d, want 404", target, rec.Code)
			}
		}
		if auth.calls != 0 {
			t.Fatalf("authentication calls = %d, want none before the canonical-path check", auth.calls)
		}
	})
	t.Run("wrong-method", func(t *testing.T) {
		handler, auth, _, _ := trustedHandlerFixture(t, validate)
		req := httptest.NewRequest(http.MethodPost, PathControlRequest, strings.NewReader(`{"controlRequestAck":{"id":"x","at":"2026-10-05T12:00:00Z"}}`))
		req.Header.Set("Authorization", "Bearer trusted-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("POST on control-request status = %d, want 404", rec.Code)
		}
		rec = getControlRequest(t, handler, PathTrustedStatus, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("GET on status status = %d, want 404", rec.Code)
		}
		if auth.calls != 0 {
			t.Fatalf("authentication calls = %d, want none for wrong methods", auth.calls)
		}
	})
	t.Run("body-rejected", func(t *testing.T) {
		handler, _, _, _ := trustedHandlerFixture(t, validate)
		rec := getControlRequest(t, handler, PathControlRequest, strings.NewReader("x"))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, body = %q; want 400 for a non-empty body on GET", rec.Code, rec.Body.String())
		}
	})
	t.Run("run-gone", func(t *testing.T) {
		handler, _, c, _ := trustedHandlerFixture(t, validate)
		run := &courier.CoderRun{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		rec := getControlRequest(t, handler, PathControlRequest, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, body = %q; want 404 for a missing run", rec.Code, rec.Body.String())
		}
	})
}

// failingGetReader fails every CoderRun read, standing in for an API server
// that cannot answer the trusted listener's live read.
type failingGetReader struct {
	client.Client
}

func (failingGetReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("api server unavailable")
}

// TestTrustedControlRequestTransientReadIsRetryable pins the availability
// split on the read route: a live read that fails without an answer is 503,
// so the caller's retry machinery treats it as transient.
func TestTrustedControlRequestTransientReadIsRetryable(t *testing.T) {
	_, _, c, identity := trustedHandlerFixture(t, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return nil
	})
	writer, err := NewStatusWriter(failingGetReader{Client: c}, c, testNamespace, testRunName)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewTrustedStatusHandler(&trustedTestAuthenticator{identity: Identity{
		RunUID: identity.RunUID, RunName: testRunName, Namespace: testNamespace,
		ControlPod: identity.ControlPod, ControlPodUID: identity.ControlPodUID,
		ServiceAccount: identity.ControlServiceAccount,
	}}, writer, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := getControlRequest(t, handler, PathControlRequest, nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %q; want 503; a read failure without an answer is availability, not denial", rec.Code, rec.Body.String())
	}
}

// TestTrustedControlRequestClosedSchema pins that the acknowledgement is part
// of the closed harness schema: an unknown field anywhere in a status patch is
// a 400, including inside controlRequestAck.
func TestTrustedControlRequestClosedSchema(t *testing.T) {
	validate := func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil }
	handler, _, _, _ := trustedHandlerFixture(t, validate)
	for _, body := range []string{
		`{"checkpoint":{"plan":"x"},"unknownField":1}`,
		`{"controlRequestAck":{"id":"softstop-1","at":"2026-10-05T12:00:00Z","bogus":true}}`,
	} {
		rec := postControlRequestAck(t, handler, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s status = %d, want 400", body, rec.Code)
		}
	}
}
