package broker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	courier "github.com/misospace/courier/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type trustedTestAuthenticator struct {
	identity Identity
	err      error
	calls    int
}

func (a *trustedTestAuthenticator) Authenticate(_ context.Context, token string) (Identity, error) {
	a.calls++
	if token != "trusted-token" {
		// A refusal, not a context failure: the handler tells a refused
		// credential apart from a request that ended before it was answered.
		return Identity{}, errors.New("token is unauthenticated")
	}
	return a.identity, a.err
}

func trustedHandlerFixture(t *testing.T, validate StatusValidator) (http.Handler, *trustedTestAuthenticator, client.Client, StatusIdentity) {
	t.Helper()
	writer, c, statusIdentity := statusFixture(t)
	identity := Identity{
		RunUID: statusIdentity.RunUID, RunName: testRunName, Namespace: testNamespace,
		ControlPod: statusIdentity.ControlPod, ControlPodUID: statusIdentity.ControlPodUID,
		ServiceAccount: statusIdentity.ControlServiceAccount,
	}
	auth := &trustedTestAuthenticator{identity: identity}
	handler, err := NewTrustedStatusHandler(auth, writer, validate)
	if err != nil {
		t.Fatal(err)
	}
	return handler, auth, c, statusIdentity
}

func TestTrustedStatusRequiresLiveWorldValidator(t *testing.T) {
	writer, _, _ := statusFixture(t)
	if _, err := NewTrustedStatusHandler(&trustedTestAuthenticator{}, writer, nil); err == nil {
		t.Fatal("constructor accepted missing live-world validator")
	}
}

func TestTrustedStatusRejectsCrossRunAndStalePod(t *testing.T) {
	validate := func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil }
	t.Run("cross-run", func(t *testing.T) {
		handler, auth, c, _ := trustedHandlerFixture(t, validate)
		auth.identity.RunUID = "run-uid-b"
		request := httptest.NewRequest(http.MethodPost, PathTrustedStatus, strings.NewReader(`{"checkpoint":{"plan":"x"}}`))
		request.Header.Set("Authorization", "Bearer trusted-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnprocessableEntity {
			t.Fatalf("response = %d, want cross-run rejection", response.Code)
		}
		other := &courier.CoderRun{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, other); err != nil {
			t.Fatal(err)
		}
		if other.Status.Checkpoint != nil {
			t.Fatalf("cross-run request wrote checkpoint: %#v", other.Status.Checkpoint)
		}
	})
	t.Run("stale-pod", func(t *testing.T) {
		handler, auth, c, statusIdentity := trustedHandlerFixture(t, validate)
		pod := &corev1.Pod{}
		key := client.ObjectKey{Namespace: testNamespace, Name: statusIdentity.ControlPod}
		if err := c.Get(context.Background(), key, pod); err != nil {
			t.Fatal(err)
		}
		pod.UID = "replacement-pod-uid"
		if err := c.Update(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, PathTrustedStatus, strings.NewReader(`{"checkpoint":{"plan":"x"}}`))
		request.Header.Set("Authorization", "Bearer trusted-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnprocessableEntity || auth.calls != 1 {
			t.Fatalf("response = %d, authentication calls = %d; want stale-pod rejection", response.Code, auth.calls)
		}
	})
}

func trustedPublicServer(t *testing.T) *Server {
	t.Helper()
	cert, key := testTLSFiles(t)
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base", WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true, SourceIssue: SourceIssue{Owner: "org", Name: "repo", Number: 1}}, serverObserver{}, serverPusher{})
	if err != nil {
		t.Fatal(err)
	}
	scratch := filepath.Join(t.TempDir(), "broker-scratch")
	if err := os.Mkdir(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Policy: engine, Authenticator: &testAuthenticator{}, Importer: &serverImporter{}, ScratchDir: scratch, TLSCertFile: cert, TLSKeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestTrustedStatusSeparateRouteAndBearerAuth(t *testing.T) {
	writer, _, _ := statusFixture(t)
	auth := &trustedTestAuthenticator{identity: Identity{RunUID: "run-uid-a", RunName: testRunName, Namespace: testNamespace, ControlPod: "control-a", ControlPodUID: "pod-uid-a", ServiceAccount: "control-sa"}}
	handler, err := NewTrustedStatusHandler(auth, writer, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, PathTrustedStatus, strings.NewReader(`{"checkpoint":{"plan":"x"}}`))
	request.Header.Set("Authorization", "Bearer model-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || auth.calls != 1 {
		t.Fatalf("trusted handler response = %d, auth calls = %d; want 401 and one live auth", response.Code, auth.calls)
	}

	publicServer := trustedPublicServer(t)
	for _, target := range []string{PathTrustedStatus, PathTrustedStatus + "/", "/trusted/v1/status/../status"} {
		request := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"checkpoint":{"plan":"x"}}`))
		request.Header.Set("Authorization", "Bearer trusted-token")
		response := httptest.NewRecorder()
		publicServer.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("public route %q response = %d, want 404", target, response.Code)
		}
	}
}

func TestTrustedStatusRejectsModelWritableFieldsAndMalformedHeartbeat(t *testing.T) {
	handler, auth, _, _ := trustedHandlerFixture(t, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil })
	for _, body := range []string{
		`{"phase":"Done"}`,
		`{"restarts":99}`,
		`{"heartbeat":{"at":"not-a-time","kind":"stream"}}`,
		`{"heartbeat":{"at":"2026-01-01T00:00:00Z","kind":"stdout"}}`,
	} {
		request := httptest.NewRequest(http.MethodPost, PathTrustedStatus, strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer trusted-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest && response.Code != http.StatusUnprocessableEntity {
			t.Errorf("body %s response = %d, want rejection", body, response.Code)
		}
	}
	if auth.calls != 4 {
		t.Fatalf("authentication calls = %d, want per-request authentication", auth.calls)
	}
}

func TestTrustedStatusAcceptsConditionalHarnessPatch(t *testing.T) {
	var checked int
	handler, _, c, _ := trustedHandlerFixture(t, func(_ context.Context, run *courier.CoderRun, _ *corev1.Pod, patch HarnessPatch) error {
		checked++
		if patch.LastCommit == nil || *patch.LastCommit != "remote-sha" {
			t.Fatalf("validator saw patch=%#v", patch)
		}
		if run.UID != "run-uid-a" {
			t.Fatalf("validator saw unexpected run UID %q", run.UID)
		}
		return nil
	})
	request := httptest.NewRequest(http.MethodPost, PathTrustedStatus, strings.NewReader(`{"checkpoint":{"plan":"keep going","completedBriefs":[{"id":"b1","summary":"done","commit":"remote-sha"}]},"lastCommit":"remote-sha"}`))
	request.Header.Set("Authorization", "Bearer trusted-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || checked != 1 {
		t.Fatalf("response = %d, validator calls = %d; want 204 and one validation", response.Code, checked)
	}
	got := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Checkpoint == nil || got.Status.Checkpoint.Plan != "keep going" || got.Status.LastCommit != "remote-sha" {
		t.Fatalf("harness patch not persisted: %#v", got.Status)
	}
}

// alwaysConflictWriter makes every status update lose the resourceVersion
// CAS race, no matter how many times the writer retries.
type alwaysConflictWriter struct {
	client.Client
}

func (w alwaysConflictWriter) Status() client.SubResourceWriter {
	return alwaysConflictStatus{SubResourceWriter: w.Client.Status()}
}

type alwaysConflictStatus struct {
	client.SubResourceWriter
}

func (w alwaysConflictStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	_ = w.SubResourceWriter.Update(ctx, obj, opts...)
	return apierrors.NewConflict(schema.GroupResource{Group: courier.GroupVersion.Group, Resource: "coderruns"}, testRunName, errors.New("simulated sustained contention"))
}

// TestTrustedStatusConflictIsTransientClassifies an exhausted CAS race as
// 503, so the caller's retry machinery treats status contention as
// transient rather than failing the dispatch as a definite denial.
func TestTrustedStatusConflictIsTransientClassifies(t *testing.T) {
	_, _, c, identity := trustedHandlerFixture(t, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return nil
	})
	writer, err := NewStatusWriter(c, alwaysConflictWriter{Client: c}, testNamespace, testRunName)
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

	patch := HarnessPatch{Heartbeat: &courier.Heartbeat{At: metav1.Now(), Kind: "tool", CoordinatorPodUID: string(identity.ControlPodUID)}}
	body, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, PathTrustedStatus, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer trusted-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; an exhausted CAS race is contention, not denial", rec.Code)
	}
}

// trustedHandlerWithValidator builds a handler whose validator returns the
// given error, on the standard fixture identity.
func trustedHandlerWithValidator(t *testing.T, c client.Client, identity StatusIdentity, validate StatusValidator) *trustedStatusHandler {
	t.Helper()
	writer, err := NewStatusWriter(c, c, testNamespace, testRunName)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewTrustedStatusHandler(&trustedTestAuthenticator{identity: Identity{
		RunUID: identity.RunUID, RunName: testRunName, Namespace: testNamespace,
		ControlPod: identity.ControlPod, ControlPodUID: identity.ControlPodUID,
		ServiceAccount: identity.ControlServiceAccount,
	}}, writer, validate)
	if err != nil {
		t.Fatal(err)
	}
	return handler.(*trustedStatusHandler)
}

func postStatusPatch(t *testing.T, handler http.Handler, identity StatusIdentity, patch HarnessPatch) int {
	t.Helper()
	body, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, PathTrustedStatus, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer trusted-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Code
}

// TestTrustedStatusBackendFailureIsRetryable pins the availability split: a
// validator whose provider read failed without an answer is transient
// availability (503), so the harness's retry machinery treats it as
// retryable instead of failing a dispatch as a definite denial.
func TestTrustedStatusBackendFailureIsRetryable(t *testing.T) {
	_, _, c, identity := trustedHandlerFixture(t, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return &StatusUnavailableError{Err: errors.New("live work ref read failed: provider unavailable")}
	})
	handler := trustedHandlerWithValidator(t, c, identity, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return &StatusUnavailableError{Err: errors.New("live work ref read failed: provider unavailable")}
	})
	code := postStatusPatch(t, handler, identity, HarnessPatch{
		LastCommit: stringPtr("published-sha"),
		Heartbeat:  &courier.Heartbeat{At: metav1.Now(), Kind: "tool", CoordinatorPodUID: string(identity.ControlPodUID)},
	})
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; a backend read failure without an answer is availability, not denial", code)
	}
}

// TestTrustedStatusDefiniteMismatchIsNonRetryable pins the other half: a
// definite live-world mismatch stays a definite 4xx denial.
func TestTrustedStatusDefiniteMismatchIsNonRetryable(t *testing.T) {
	_, _, c, identity := trustedHandlerFixture(t, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return errors.New("lastCommit does not match the live work ref tip")
	})
	handler := trustedHandlerWithValidator(t, c, identity, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error {
		return errors.New("lastCommit does not match the live work ref tip")
	})
	code := postStatusPatch(t, handler, identity, HarnessPatch{
		LastCommit: stringPtr("published-sha"),
		Heartbeat:  &courier.Heartbeat{At: metav1.Now(), Kind: "tool", CoordinatorPodUID: string(identity.ControlPodUID)},
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; a definite live-world mismatch is a denial, not availability", code)
	}
}


// TestTrustedStatusIdentityFailureSplit pins the availability split at the
// authentication boundary: an identity backend that failed to answer is
// transient (503), so the harness retries the status write, while a definite
// refusal stays 401 and is never retried.
func TestTrustedStatusIdentityFailureSplit(t *testing.T) {
	validate := func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil }
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "transient backend failure", err: &IdentityUnavailableError{Err: errors.New("review control token: api server unavailable")}, want: http.StatusServiceUnavailable},
		{name: "definite refusal", err: errors.New("token is unauthenticated or lacks broker audience"), want: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, auth, _, _ := trustedHandlerFixture(t, validate)
			auth.err = tt.err
			request := httptest.NewRequest(http.MethodPost, PathTrustedStatus, strings.NewReader(`{"checkpoint":{"plan":"x"}}`))
			request.Header.Set("Authorization", "Bearer trusted-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tt.want {
				t.Fatalf("response = %d, want %d; body = %q", response.Code, tt.want, response.Body.String())
			}
			if auth.calls != 1 {
				t.Fatalf("authentication calls = %d, want one live auth", auth.calls)
			}
		})
	}
}

func stringPtr(v string) *string { return &v }

// stalledValidator stands in for the live forge read a lastCommit
// confirmation depends on: it answers only when the handler's budget ends,
// and it reports the failure the way the production validator does — as
// availability, so the answer stays retryable. The fallback keeps a
// regression from hanging the test: without a budget the validator reports
// that it never saw one.
func stalledValidator(ctx context.Context, _ *courier.CoderRun, _ *corev1.Pod, _ HarnessPatch) error {
	select {
	case <-ctx.Done():
		return &StatusUnavailableError{Err: fmt.Errorf("live work ref read failed: %w", ctx.Err())}
	case <-time.After(2 * time.Second):
		return errors.New("stalled backend never observed the handler budget")
	}
}

// TestTrustedStatusBudgetFitsInsideWriteDeadline pins the budget the trusted
// status handler spends on its own work. The listener's write deadline starts
// when the request headers are read, so the handler's budget must sit inside
// that deadline with the response reserve in between: a healthy status write
// can then never be cut off by the write bound, and a request that exhausts
// its budget answers a retryable 503 instead of a truncated response.
func TestTrustedStatusBudgetFitsInsideWriteDeadline(t *testing.T) {
	if StatusRequestBudget <= 0 || StatusResponseReserve <= 0 {
		t.Fatalf("budget = %v, reserve = %v; both must be positive", StatusRequestBudget, StatusResponseReserve)
	}
	if StatusRequestBudget+StatusResponseReserve != InboundWriteTimeout {
		t.Fatalf("budget %v + reserve %v = %v, want the listener's write bound %v", StatusRequestBudget, StatusResponseReserve, StatusRequestBudget+StatusResponseReserve, InboundWriteTimeout)
	}
	if StatusRequestBudget >= InboundWriteTimeout {
		t.Fatalf("budget = %v, want strictly inside the write bound %v", StatusRequestBudget, InboundWriteTimeout)
	}
	// The handler must spend that budget: a handler built with a different
	// one would let the write deadline end a healthy status write.
	writer, _, _ := statusFixture(t)
	handler, err := NewTrustedStatusHandler(&trustedTestAuthenticator{}, writer, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got := handler.(*trustedStatusHandler).budget; got != StatusRequestBudget {
		t.Fatalf("handler budget = %v, want the exported %v", got, StatusRequestBudget)
	}
}

// TestTrustedStatusBudgetExhaustionIsRetryable: a backend that cannot answer
// inside the handler's budget ends the request with a complete, retryable
// answer, so the caller's retry machinery retries the write instead of
// treating an unproven update as a definite denial.
func TestTrustedStatusBudgetExhaustionIsRetryable(t *testing.T) {
	for _, test := range []struct {
		name     string
		validate StatusValidator
	}{
		// The production shape: the live forge read failed without an
		// answer, so the validator reports availability.
		{name: "typed-availability", validate: stalledValidator},
		// A validator that reports the ended request as a bare context
		// error must not turn it into a definite denial either.
		{name: "bare-context-error", validate: func(ctx context.Context, _ *courier.CoderRun, _ *corev1.Pod, _ HarnessPatch) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
				return errors.New("stalled backend never observed the handler budget")
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, c, identity := statusFixture(t)
			handler := trustedHandlerWithValidator(t, c, identity, test.validate)
			handler.budget = 100 * time.Millisecond
			code := postStatusPatch(t, handler, identity, HarnessPatch{LastCommit: stringPtr("published-sha")})
			if code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503; an exhausted budget is availability, not denial", code)
			}
			run := &courier.CoderRun{}
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, run); err != nil {
				t.Fatal(err)
			}
			if run.Status.LastCommit != "" {
				t.Fatalf("an unproven update was persisted: %#v", run.Status)
			}
		})
	}
}

// TestTrustedStatusAnswersBeforeWriteDeadline proves the property the
// listener's write bound depends on: the handler's own budget ends the
// request before the server's write deadline, so the caller receives a whole
// answer. The window is scaled down from production, but the shape is the
// production one: the budget ends the work, the reserve covers the answer.
func TestTrustedStatusAnswersBeforeWriteDeadline(t *testing.T) {
	_, c, identity := statusFixture(t)
	handler := trustedHandlerWithValidator(t, c, identity, stalledValidator)
	handler.budget = 200 * time.Millisecond
	server := httptest.NewUnstartedServer(handler)
	server.Config.WriteTimeout = handler.budget + 100*time.Millisecond
	server.Start()
	defer server.Close()

	body, err := json.Marshal(HarnessPatch{LastCommit: stringPtr("published-sha")})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+PathTrustedStatus, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer trusted-token")
	started := time.Now()
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("status request failed: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; an exhausted budget is availability, not denial", response.StatusCode)
	}
	if string(payload) != "status update unavailable\n" {
		t.Fatalf("body = %q, want the whole retryable answer", payload)
	}
	if elapsed := time.Since(started); elapsed >= server.Config.WriteTimeout {
		t.Fatalf("answer took %v, want the handler's budget to end the request inside the %v write deadline", elapsed, server.Config.WriteTimeout)
	}
}

// TestTrustedStatusStalledBodyCannotOutliveWriteDeadline pins the body read
// inside the same budget: a client that sends its headers and then stalls its
// body must not hold the request past the listener's write deadline, because
// the answer would be written into an expired deadline and the caller would
// see a truncated response instead of the retryable answer.
func TestTrustedStatusStalledBodyCannotOutliveWriteDeadline(t *testing.T) {
	_, c, identity := statusFixture(t)
	handler := trustedHandlerWithValidator(t, c, identity, func(context.Context, *courier.CoderRun, *corev1.Pod, HarnessPatch) error { return nil })
	handler.budget = 200 * time.Millisecond
	server := httptest.NewUnstartedServer(handler)
	server.Config.WriteTimeout = handler.budget + 100*time.Millisecond
	server.Start()
	defer server.Close()

	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The headers arrive; the body never does.
	if _, err := conn.Write([]byte("POST " + PathTrustedStatus + " HTTP/1.1\r\nHost: broker.test\r\nAuthorization: Bearer trusted-token\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+PathTrustedStatus, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), request)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; an unreadable body is an incomplete request, not a denial", response.StatusCode)
	}
	if string(payload) != "status update unavailable\n" {
		t.Fatalf("body = %q, want the whole retryable answer", payload)
	}
}

// TestStatusWriterBackendReadFailureIsTyped pins the writer-level split: an
// API-server read that fails without an answer is StatusUnavailableError
// (transient), while a missing run is a definite error.
func TestStatusWriterBackendReadFailureIsTyped(t *testing.T) {
	w, c, identity := statusFixture(t)
	w.reader = failingListReader{Client: c}
	err := w.Write(context.Background(), identity, HarnessPatch{ClearOperation: "shell.b1.aaaa"}, nil)
	var unavailable *StatusUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("error = %v, want StatusUnavailableError for a failing backend read", err)
	}
}
