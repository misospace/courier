package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	engine, err := NewPolicyEngine(Policy{RunUID: "run-uid", Mode: ModeResolveIssue, Provider: "test", BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base", WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-1", WorkInitiallyAbsent: true}, serverObserver{}, serverPusher{})
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
