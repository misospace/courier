package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		return Identity{}, context.Canceled
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
func trustedHandlerWithValidator(t *testing.T, c client.Client, identity StatusIdentity, validate StatusValidator) http.Handler {
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
	return handler
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

func stringPtr(v string) *string { return &v }

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
