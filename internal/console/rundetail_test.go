package console

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/readapi"
)

func TestRunDetailWireConstantsAndNameValidation(t *testing.T) {
	if backendRunDetailPrefix != readapi.PathRun[:strings.LastIndex(readapi.PathRun, "{")] {
		t.Fatalf("detail prefix = %q, expected prefix of %q", backendRunDetailPrefix, readapi.PathRun)
	}
	if kindRunDetail != readapi.KindRunDetail {
		t.Fatalf("detail kind = %q, want %q", kindRunDetail, readapi.KindRunDetail)
	}
	for _, name := range []string{"a", "run-1", "a.b", strings.Repeat("a", 63), strings.Repeat("a", 63) + "." + strings.Repeat("b", 63)} {
		if !validRunName(name) {
			t.Errorf("validRunName(%q) = false", name)
		}
	}
	bad := []string{"../secret", "a/b", "a b", "A-upper", "-lead", "trail-", "", strings.Repeat("a", 254), strings.Repeat("a", 64), "a..b"}
	for _, name := range bad {
		if validRunName(name) {
			t.Errorf("validRunName(%q) = true", name)
		}
	}
	for path, want := range map[string]bool{
		"/v1/runs/ok-name":    true,
		"/v1/runs/../healthz": false,
		"/v1/runs/x/../../v2": false,
	} {
		if got := allowlistedBackendPath(path); got != want {
			t.Errorf("allowlistedBackendPath(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRunDetailPageAndBackendRequest(t *testing.T) {
	const token = "read-token"
	body := `{"apiVersion":"courier.misospace.dev/v1","kind":"RunDetail","payload":{"name":"run-one","namespace":"ns","phase":"Done","terminal":true,"ageSeconds":65,"source":"dispatch","lane":"fast","prNumber":42,"branch":"work","headSHA":"abc123","restarts":2,"terminalReason":"finished","heartbeat":{"hasHeartbeat":true,"lastHeartbeatAt":"2026-01-01T00:00:00Z","freshness":"Fresh"},"admission":{"disagreement":false},"checkpoint":{"present":true,"briefCount":2},"publication":{"lastCommit":"abc123","hasCheckFingerprint":true},"ciPark":{"head":"abc123","parkedAt":"2026-01-01T00:00:00Z","attempts":1,"lastOutcome":"repushed"},"lifecycleReport":{"conditionPresent":true,"conditionStatus":"True","hasActiveRetryToken":false,"hasLastRetryToken":true},"coordinator":{"category":"workload-failure","exitCode":1,"outcome":"failed","reason":"exit","observedAt":"2026-01-01T00:00:00Z","freshness":"Fresh","container":{"name":"coordinator","restartCount":2,"terminationReason":"Error"}},"reap":{"markerAnnotation":"done-at","reason":"resolved","eligibleByPhase":true,"holds":["source report not settled"]},"workspaceSafety":{"state":"pushed","branch":"work","head":"abc123"},"nativeResultKind":"published","conditions":[{"type":"LifecycleReported","status":"True","observedAt":"2026-01-01T00:00:00Z"}],"timeline":[{"at":"2026-01-01T00:00:00Z","label":"created"},{"at":"2026-01-01T00:01:00Z","label":"finished","detail":"Done"}],"timelineTruncated":false}}`
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, body, capture)
	defer backend.Close()
	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, token)
	client := login(t, base, secrets.token)
	resp, page := get(t, client, base+"/runs/run-one")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	for _, want := range []string{"run-one", "dispatch", "fast", "Done", "1m", "#42", "abc123", "Checkpoint", "completed briefs", "CI park", "repushed", "source report not settled", "created", "finished"} {
		if !strings.Contains(page, want) {
			t.Errorf("page missing %q", want)
		}
	}
	if strings.Contains(page, "<script") || strings.Contains(page, "Run not found") {
		t.Fatalf("page has script or not-found state: %s", page)
	}
	if capture.method != http.MethodGet || capture.path != "/v1/runs/run-one" || capture.auth != "Bearer "+token {
		t.Fatalf("backend request = %s %s %q", capture.method, capture.path, capture.auth)
	}
}

func TestRunDetailNotFoundAndErrors(t *testing.T) {
	cases := []struct {
		name   string
		url    string
		token  string
		status int
		body   string
		want   string
	}{
		{"unconfigured", "", "tok", 0, "", "not configured for this console"},
		{"no token", "http://127.0.0.1:1", "", 0, "", "no token"},
		{"401", "", "tok", 401, `{}`, "refused this console"},
		{"503", "", "tok", 503, `{}`, "read budget"},
		{"wrong kind", "", "tok", 200, `{"kind":"Overview","payload":{}}`, "not with run detail"},
		{"garbage", "", "tok", 200, `<html>bad</html>`, "not with run detail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backendURL := tc.url
			if tc.status != 0 {
				stub := newStubReadAPI(t, tc.status, tc.body, nil)
				defer stub.Close()
				backendURL = stub.URL
			}
			secrets := newTestSecrets(t)
			_, base := newTestServerWithToken(t, secrets, backendURL, tc.token)
			client := login(t, base, secrets.token)
			_, page := get(t, client, base+"/runs/run-one")
			if !strings.Contains(page, tc.want) {
				t.Fatalf("page missing %q:\n%s", tc.want, page)
			}
		})
	}

	stub := newStubReadAPI(t, http.StatusNotFound, `{}`, nil)
	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, stub.URL, "tok")
	client := login(t, base, secrets.token)
	_, page := get(t, client, base+"/runs/run-one")
	stub.Close()
	if !strings.Contains(page, "was not found in this namespace") || !strings.Contains(page, `href="/runs"`) || strings.Contains(page, "state-error") {
		t.Fatalf("404 did not render not-found state:\n%s", page)
	}
}

func TestRunDetailTimeoutAndUnreachableMessages(t *testing.T) {
	secrets := newTestSecrets(t)
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := closed.URL
	closed.Close()
	_, base := newTestServerWithToken(t, secrets, closedURL, "tok")
	client := login(t, base, secrets.token)
	_, page := get(t, client, base+"/runs/run-one")
	if !strings.Contains(page, "could not be reached") {
		t.Fatalf("unreachable message missing: %s", page)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(100 * time.Millisecond); fmt.Fprint(w, `{}`) }))
	defer server.Close()
	console, err := NewServer(Config{Mode: AuthModeToken, SessionKey: []byte(secrets.sessionKey), OperatorTokens: [][]byte{[]byte(secrets.token)}, BackendURL: server.URL, BackendToken: []byte("tok"), BackendTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	httpConsole := httptest.NewServer(console.Handler())
	defer httpConsole.Close()
	timeoutClient := login(t, httpConsole.URL, secrets.token)
	_, page = get(t, timeoutClient, httpConsole.URL+"/runs/run-one")
	if !strings.Contains(page, "request timed out") {
		t.Fatalf("timeout message missing: %s", page)
	}
}
