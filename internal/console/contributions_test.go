package console

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misospace/courier/internal/source/adhoc"
)

// fakeContributeStatus is a stub status read for the handlers: it returns a
// fixed result and records the calls that reached it, so tests can pin that
// invalid input never reaches the backend.
type fakeContributeStatus struct {
	status  contributionStatus
	err     error
	calls   []string
	lastNS  string
	lastRun string
}

func (f *fakeContributeStatus) Status(_ context.Context, namespace, run string) (contributionStatus, error) {
	f.calls = append(f.calls, namespace+"/"+run)
	f.lastNS, f.lastRun = namespace, run
	if f.err != nil {
		return contributionStatus{}, f.err
	}
	return f.status, nil
}

// contributeTestNow is the pinned clock of every status test server. It sits
// ten seconds after the fixtures' observedAt stamp, so the staleness window
// reads the fixture answers as fresh; a test that wants a stale answer sets
// observedAt itself relative to this instant.
var contributeTestNow = time.Date(2026, 11, 20, 9, 31, 10, 0, time.UTC)

// newContributeServer returns a logged-in token-mode console whose status
// read is the stub, plus the base URL.
func newContributeServer(t *testing.T, secrets *testSecrets, stub contributeStatusClient) (*Server, string) {
	t.Helper()
	server, base := newTestServer(t, secrets, "")
	server.contributions = stub
	server.auth.now = func() time.Time { return contributeTestNow }
	return server, base
}

func fullContributionStatus() contributionStatus {
	return contributionStatus{
		Run:       "courier-3f2a",
		Namespace: "contrib",
		Lane:      "default",
		BaseRepo:  "misospace/courier",
		WorkRepo:  "contributor/fork",
		Phase:     "Verifying",
		CIParked:  true,
		Publication: &contributionPublication{
			Branch:   "courier-3f2a",
			HeadRepo: "contributor/fork",
			HeadSHA:  "0f4a9c8b",
			PR:       "https://github.com/misospace/courier/pull/42",
		},
		AdmittedAt:     "2026-11-20T09:00:00Z",
		StartedAt:      "2026-11-20T09:01:00Z",
		PhaseEnteredAt: "2026-11-20T09:30:00Z",
		ObservedAt:     "2026-11-20T09:31:00Z",
	}
}

// --- wire-contract pins ---------------------------------------------------

// TestContributeStatusContractPins keeps the console's mirror constants from
// drifting from the packages that own the wire shapes. The console imports
// them only here, never in production code.
func TestContributeStatusContractPins(t *testing.T) {
	if contributeStatusPrefix != adhoc.PathContributions+"/" {
		t.Errorf("contributeStatusPrefix = %q, want %q", contributeStatusPrefix, adhoc.PathContributions+"/")
	}
	if contributionSource != adhoc.SourceName {
		t.Errorf("contributionSource = %q, want adhoc.SourceName %q", contributionSource, adhoc.SourceName)
	}
	if phaseContributeUnknown != adhoc.PhaseUnknown {
		t.Errorf("phaseContributeUnknown = %q, want adhoc.PhaseUnknown %q", phaseContributeUnknown, adhoc.PhaseUnknown)
	}
}

// --- input validation -------------------------------------------------------

func TestValidContributeRunName(t *testing.T) {
	valid := []string{"courier-abc", "a", "a.b.c", "run-2026-11-20.t1", strings.Repeat("a", 253)}
	for _, name := range valid {
		if !validContributeRunName(name) {
			t.Errorf("validContributeRunName(%q) = false, want true", name)
		}
	}
	invalid := []string{"", "BAD", "bad_name", "-bad", "bad-", ".bad", "bad.", "bad name", "bad/slash", "%2e", strings.Repeat("a", 254)}
	for _, name := range invalid {
		if validContributeRunName(name) {
			t.Errorf("validContributeRunName(%q) = true, want false", name)
		}
	}
}

func TestValidContributeNamespace(t *testing.T) {
	for _, ns := range []string{"", "contrib", "a", "kube-system", strings.Repeat("a", 63)} {
		if !validContributeNamespace(ns) {
			t.Errorf("validContributeNamespace(%q) = false, want true", ns)
		}
	}
	for _, ns := range []string{"Contrib", "bad_ns", "-bad", "bad-", "bad.name", strings.Repeat("a", 64)} {
		if validContributeNamespace(ns) {
			t.Errorf("validContributeNamespace(%q) = true, want false", ns)
		}
	}
}

func TestSafeContributionURL(t *testing.T) {
	safe := map[string]string{
		"https://github.com/o/r/pull/1":               "https://github.com/o/r/pull/1",
		"https://ghe.example.internal/o/r/pull/1?x=1": "https://ghe.example.internal/o/r/pull/1?x=1",
	}
	for raw, want := range safe {
		if got := safeContributionURL(raw); got != want {
			t.Errorf("safeContributionURL(%q) = %q, want %q", raw, got, want)
		}
	}
	for _, raw := range []string{
		"", "http://github.com/o/r/pull/1", "javascript:alert(1)", "data:text/html;base64,xx",
		"https://user@github.com/o/r/pull/1", "//github.com/o/r", "/pull/1", "github.com/o/r",
	} {
		if got := safeContributionURL(raw); got != "" {
			t.Errorf("safeContributionURL(%q) = %q, want rejected", raw, got)
		}
	}
}

func TestContributePhaseLabel(t *testing.T) {
	cases := map[string]string{
		"Pending":         "Admitted, waiting to start",
		"Claimed":         "Executing",
		"Running":         "Executing",
		"Verifying":       "Verifying",
		"AwaitingReview":  "Awaiting review",
		"HandedOff":       "Awaiting review",
		"NeedsHuman":      "Operator action needed",
		"BlockedExternal": "Blocked outside Courier",
		"Done":            "Finished",
		"AlreadyDone":     "Finished (already done)",
		"Failed":          "Failed",
		"unknown":         "No phase recorded yet",
		"":                "No phase recorded yet",
	}
	for phase, want := range cases {
		label, class := contributePhaseLabel(phase)
		if label != want {
			t.Errorf("contributePhaseLabel(%q) label = %q, want %q", phase, label, want)
		}
		if !strings.HasPrefix(class, "badge ") {
			t.Errorf("contributePhaseLabel(%q) class = %q, want a badge class", phase, class)
		}
	}
	label, class := contributePhaseLabel("Teleported")
	if label != "Unknown (projected: Teleported)" || class != "badge badge-unknown" {
		t.Errorf("out-of-vocabulary phase = %q/%q, want the raw word marked unknown", label, class)
	}
	for _, phase := range []string{"Done", "AlreadyDone"} {
		label, _ := contributePhaseLabel(phase)
		if strings.Contains(strings.ToLower(label), "merged") || strings.Contains(strings.ToLower(label), "success") {
			t.Errorf("phase %q rendered a merged/success verdict: %q", phase, label)
		}
	}
}

func TestContributeRefreshSeconds(t *testing.T) {
	cases := []struct {
		phase    string
		ciParked bool
		want     int
	}{
		{"Done", false, 0}, {"AlreadyDone", false, 0}, {"Failed", false, 0},
		{"Done", true, 0}, // a retained CI park never reactivates a terminal run
		{"AwaitingReview", false, contributeStatusParkedSeconds},
		{"HandedOff", false, contributeStatusParkedSeconds},
		{"NeedsHuman", false, contributeStatusParkedSeconds},
		{"BlockedExternal", false, contributeStatusParkedSeconds},
		{"Pending", false, contributeStatusRefreshSeconds},
		{"Claimed", false, contributeStatusRefreshSeconds},
		{"Running", false, contributeStatusRefreshSeconds},
		{"Verifying", false, contributeStatusRefreshSeconds},
		{"Verifying", true, contributeStatusParkedSeconds}, // a CI-parked run waits outside the pipeline
		{"unknown", false, contributeStatusRefreshSeconds},
		{"", false, contributeStatusRefreshSeconds},
		{"Teleported", false, contributeStatusParkedSeconds},
	}
	for _, tc := range cases {
		if got := contributeRefreshSeconds(tc.phase, tc.ciParked); got != tc.want {
			t.Errorf("contributeRefreshSeconds(%q, %v) = %d, want %d", tc.phase, tc.ciParked, got, tc.want)
		}
	}
}

func TestContributePhaseAge(t *testing.T) {
	if got := contributePhaseAge("2026-11-20T09:00:00Z", "2026-11-20T09:30:00Z"); got != "30m" {
		t.Errorf("phase age = %q, want 30m", got)
	}
	if got := contributePhaseAge("2026-11-20T10:00:00Z", "2026-11-20T09:00:00Z"); got != "0s" {
		t.Errorf("skewed phase age = %q, want the clamped 0s", got)
	}
	for _, pair := range [][2]string{{"", ""}, {"nope", "2026-11-20T09:00:00Z"}, {"2026-11-20T09:00:00Z", "nope"}} {
		if got := contributePhaseAge(pair[0], pair[1]); got != "" {
			t.Errorf("contributePhaseAge(%q, %q) = %q, want empty", pair[0], pair[1], got)
		}
	}
}

// --- client -----------------------------------------------------------------

func TestNewHTTPContributeStatusClientRejectsBadURLs(t *testing.T) {
	for _, bad := range []string{"ftp://host", "http://user:pw@host", "http://host/?a=b", "http://host/#frag", "http://", "/just/a/path", ":x"} {
		if _, err := newHTTPContributeStatusClient(bad, []byte("t")); err == nil {
			t.Errorf("newHTTPContributeStatusClient(%q) accepted a hostile or malformed URL", bad)
		}
	}
	client, err := newHTTPContributeStatusClient("http://courier-contribute-api/", []byte("t"))
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "http://courier-contribute-api" {
		t.Errorf("baseURL = %q, want the trailing slash trimmed", client.baseURL)
	}
}

func TestContributeStatusClientRequestShape(t *testing.T) {
	const readToken = "status-read-token-xyz"
	var gotAuth, gotPath, gotQuery, gotMethod string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath, gotQuery, gotMethod = r.Header.Get("Authorization"), r.URL.Path, r.URL.RawQuery, r.Method
		_ = json.NewEncoder(w).Encode(map[string]any{
			"run": "courier-abc.1", "namespace": "contrib", "phase": "Pending", "observedAt": "2026-11-20T09:00:00Z",
		})
	}))
	t.Cleanup(upstream.Close)
	client, err := newHTTPContributeStatusClient(upstream.URL, []byte(readToken))
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Status(context.Background(), "contrib", "courier-abc.1")
	if err != nil {
		t.Fatal(err)
	}
	if status.Run != "courier-abc.1" || status.Phase != "Pending" {
		t.Fatalf("decoded status = %+v", status)
	}
	if gotMethod != http.MethodGet || gotPath != "/v1/contributions/courier-abc.1" {
		t.Errorf("request = %s %s, want GET /v1/contributions/courier-abc.1", gotMethod, gotPath)
	}
	if gotQuery != "namespace=contrib" {
		t.Errorf("query = %q, want namespace=contrib", gotQuery)
	}
	if gotAuth != "Bearer "+readToken {
		t.Errorf("authorization = %q", gotAuth)
	}
	if _, err := client.Status(context.Background(), "", "courier-abc.1"); err != nil {
		t.Fatal(err)
	}
	if gotQuery != "" {
		t.Errorf("empty namespace must omit the parameter, got query %q", gotQuery)
	}
}

func TestContributeStatusClientNeverSendsAnInvalidRun(t *testing.T) {
	reached := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}))
	t.Cleanup(upstream.Close)
	client, err := newHTTPContributeStatusClient(upstream.URL, []byte("t"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Status(context.Background(), "", "Bad Name"); err == nil || reached {
		t.Fatalf("invalid run name must fail locally without a request (reached=%v, err=%v)", reached, err)
	}
}

func TestContributeStatusClientNeverFollowsRedirects(t *testing.T) {
	// The status bearer must never ride a redirect: Go's default policy
	// keeps Authorization for a same-host or subdomain target, so the
	// client must not follow any redirect at all. The landing path would
	// record whatever it receives; reaching it fails the test.
	var landedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/landed" {
			landedAuth = r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"run":"courier-abc","observedAt":"2026-11-20T09:00:00Z"}`))
			return
		}
		http.Redirect(w, r, "/landed", http.StatusFound)
	}))
	t.Cleanup(upstream.Close)
	client, err := newHTTPContributeStatusClient(upstream.URL, []byte("status-read-token-xyz"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Status(context.Background(), "", "courier-abc")
	var statusErr *ContributeStatusError
	if !errors.As(err, &statusErr) || statusErr.Status != http.StatusFound {
		t.Fatalf("redirect = %v, want *ContributeStatusError with status 302", err)
	}
	if landedAuth != "" {
		t.Errorf("the redirect was followed and carried authorization %q", landedAuth)
	}
}

func TestContributeStatusClientStatusMapping(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusUnauthorized, http.StatusServiceUnavailable, http.StatusInternalServerError} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"body material that must never leak"}`))
		}))
		client, err := newHTTPContributeStatusClient(upstream.URL, []byte("t"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Status(context.Background(), "", "courier-abc")
		upstream.Close()
		var statusErr *ContributeStatusError
		if !errors.As(err, &statusErr) || statusErr.Status != status {
			t.Fatalf("status %d mapped to %v, want *ContributeStatusError with that status", status, err)
		}
		if strings.Contains(err.Error(), "body material") {
			t.Fatalf("error leaked the response body: %v", err)
		}
	}
}

func TestContributeStatusClientUnexpectedBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "huge"):
			_, _ = w.Write([]byte(strings.Repeat("x", maxContributeStatusBody+1)))
		case strings.HasSuffix(r.URL.Path, "empty-object"):
			// A misconfigured status URL pointed at some other service:
			// valid JSON, but not a contribution status.
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "other-run"):
			_, _ = w.Write([]byte(`{"run":"some-other-run","observedAt":"2026-11-20T09:00:00Z"}`))
		case strings.HasSuffix(r.URL.Path, "no-observed"):
			_, _ = w.Write([]byte(`{"run":"courier-no-observed","phase":"Pending"}`))
		default:
			_, _ = w.Write([]byte("<html>not json</html>"))
		}
	}))
	t.Cleanup(upstream.Close)
	client, err := newHTTPContributeStatusClient(upstream.URL, []byte("t"))
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range []string{"courier-ok", "courier-huge", "courier-empty-object", "courier-other-run", "courier-no-observed"} {
		if _, err := client.Status(context.Background(), "", run); !errors.Is(err, ErrContributeStatusUnexpected) {
			t.Errorf("%s: err = %v, want ErrContributeStatusUnexpected", run, err)
		}
	}
}

// TestContributeStatusWrongShapeNamespaceAndStamp pins the extended
// wrong-shape boundary: with a pinned request, a namespace echo that differs
// from the pin is not the requested run, a missing namespace echo is the same
// misconfiguration as a bare {}, and a malformed observedAt is rejected even
// though it is non-empty. Without a pin the mismatch cannot exist: the read
// accepts whatever canonical echo the endpoint supplies, which is what the
// detail page then pins and records.
func TestContributeStatusWrongShapeNamespaceAndStamp(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "wrong-namespace"):
			_, _ = w.Write([]byte(`{"run":"courier-wrong-namespace","namespace":"elsewhere","observedAt":"2026-11-20T09:00:00Z"}`))
		case strings.HasSuffix(r.URL.Path, "no-namespace"):
			_, _ = w.Write([]byte(`{"run":"courier-no-namespace","observedAt":"2026-11-20T09:00:00Z"}`))
		case strings.HasSuffix(r.URL.Path, "bad-stamp"):
			_, _ = w.Write([]byte(`{"run":"courier-bad-stamp","namespace":"contrib","observedAt":"last tuesday"}`))
		default:
			_, _ = w.Write([]byte(`{"run":"courier-healthy","namespace":"contrib","phase":"Pending","observedAt":"2026-11-20T09:00:00Z"}`))
		}
	}))
	t.Cleanup(upstream.Close)
	client, err := newHTTPContributeStatusClient(upstream.URL, []byte("t"))
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range []string{"courier-wrong-namespace", "courier-no-namespace", "courier-bad-stamp"} {
		if _, err := client.Status(context.Background(), "contrib", run); !errors.Is(err, ErrContributeStatusUnexpected) {
			t.Errorf("%s pinned to contrib: err = %v, want ErrContributeStatusUnexpected", run, err)
		}
	}
	if _, err := client.Status(context.Background(), "", "courier-wrong-namespace"); err != nil {
		t.Errorf("unpinned wrong-namespace: err = %v, want the echoed namespace accepted", err)
	}
	if _, err := client.Status(context.Background(), "contrib", "courier-healthy"); err != nil {
		t.Errorf("healthy: err = %v", err)
	}
}

// TestContributionDetailStaleRead pins the stale-response handling #830
// requires: a well-shaped read whose observedAt sits outside the staleness
// window — old (a cached or stalled upstream) or ahead of the console's clock
// (severe skew) — still renders, but is labelled stale instead of reading as
// a live observation. A healthy stamp renders no label.
func TestContributionDetailStaleRead(t *testing.T) {
	for _, tc := range []struct {
		name       string
		offset     time.Duration
		wantStaled bool
	}{
		{"fresh", -10 * time.Second, false},
		{"old", -10 * time.Minute, true},
		{"future", 10 * time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			secrets := newTestSecrets(t)
			status := fullContributionStatus()
			status.ObservedAt = contributeTestNow.Add(tc.offset).Format(time.RFC3339)
			stub := &fakeContributeStatus{status: status}
			_, base := newContributeServer(t, secrets, stub)
			client := login(t, base, secrets.token)
			resp, body := get(t, client, base+"/contributions/courier-3f2a")
			if resp.StatusCode != http.StatusOK || stub.calls == nil {
				t.Fatalf("status = %d, calls = %v", resp.StatusCode, stub.calls)
			}
			if got := strings.Contains(body, "This read is stale"); got != tc.wantStaled {
				t.Errorf("stale label present = %v, want %v:\n%s", got, tc.wantStaled, body)
			}
		})
	}
}

// --- handlers ----------------------------------------------------------------

// TestNewServerRejectsInvalidContributeStatusURL pins fail-closed
// construction: a malformed status URL is a startup error, never a runtime
// surprise that sends requests somewhere unintended.
func TestNewServerRejectsInvalidContributeStatusURL(t *testing.T) {
	secrets := newTestSecrets(t)
	_, err := NewServer(Config{
		Mode:                AuthModeToken,
		SessionKey:          []byte(secrets.sessionKey),
		OperatorTokens:      [][]byte{[]byte(secrets.token)},
		ContributeStatusURL: "http://user:secret@host",
	})
	if err == nil {
		t.Fatal("NewServer accepted a credential-bearing contribution status URL")
	}
}

func TestContributionRoutesRequireOperator(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newTestServer(t, secrets, "")
	for _, path := range []string{"/contributions", "/contributions/courier-abc"} {
		resp, body := get(t, nil, base+path)
		if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(resp.Header.Get("Location"), "/login") {
			t.Fatalf("GET %s unauthenticated = %d %q, want a redirect to /login", path, resp.StatusCode, body)
		}
	}
}

func TestContributionPagesNotConfigured(t *testing.T) {
	secrets := newTestSecrets(t)
	server, base := newTestServer(t, secrets, "")
	server.contributions = nil
	client := login(t, base, secrets.token)
	for _, path := range []string{"/contributions", "/contributions/courier-abc"} {
		resp, body := get(t, client, base+path)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200 with the not-configured state", path, resp.StatusCode)
		}
		if !strings.Contains(body, "not configured") {
			t.Fatalf("GET %s does not state the not-configured condition:\n%s", path, body)
		}
	}
}

func TestContributionDetailFound(t *testing.T) {
	secrets := newTestSecrets(t)
	stub := &fakeContributeStatus{status: fullContributionStatus()}
	_, base := newContributeServer(t, secrets, stub)
	client := login(t, base, secrets.token)
	resp, body := get(t, client, base+"/contributions/courier-3f2a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if stub.lastNS != "" || stub.lastRun != "courier-3f2a" {
		t.Errorf("backend call = %q/%q, want the path run with no namespace", stub.lastNS, stub.lastRun)
	}
	for _, want := range []string{
		"Verifying", "waits in the CI park",
		`href="https://github.com/misospace/courier/pull/42"`, "contributor/fork", "courier-3f2a",
		`href="https://github.com/contributor/fork"`,
		`href="https://github.com/misospace/courier/pull/42/commit/0f4a9c8b"`,
		`href="https://github.com/misospace/courier"`,
		"does not track whether it was merged",
		"2026-11-20T09:31:00Z", // observedAt verbatim
		`content="60"`,         // a CI-parked Verifying run polls at the parked interval
	} {
		if !strings.Contains(body, want) {
			t.Errorf("detail page missing %q:\n%s", want, body)
		}
	}
	// The canonical namespace from the response is what the page names.
	if !strings.Contains(body, "contrib") {
		t.Errorf("detail page does not show the response namespace:\n%s", body)
	}
}

func TestContributionDetailActiveVerifyingPollsFast(t *testing.T) {
	secrets := newTestSecrets(t)
	status := fullContributionStatus()
	status.CIParked = false
	stub := &fakeContributeStatus{status: status}
	_, base := newContributeServer(t, secrets, stub)
	client := login(t, base, secrets.token)
	_, body := get(t, client, base+"/contributions/courier-3f2a")
	if !strings.Contains(body, `content="15"`) {
		t.Errorf("an unparked Verifying run must poll at the active interval:\n%s", body)
	}
}

func TestContributionDetailNamespacePinned(t *testing.T) {
	secrets := newTestSecrets(t)
	stub := &fakeContributeStatus{status: fullContributionStatus()}
	_, base := newContributeServer(t, secrets, stub)
	client := login(t, base, secrets.token)
	_, body := get(t, client, base+"/contributions/courier-3f2a?namespace=contrib")
	if stub.lastNS != "contrib" {
		t.Errorf("namespace = %q, want the pinned query value forwarded", stub.lastNS)
	}
	if !strings.Contains(body, "Contributions") {
		t.Errorf("unexpected page body:\n%s", body)
	}
}

func TestContributionDetailTerminalStopsRefreshing(t *testing.T) {
	secrets := newTestSecrets(t)
	stub := &fakeContributeStatus{status: contributionStatus{
		Run: "courier-3f2a", Namespace: "contrib", Phase: "Failed",
		Cause:      &contributionCause{Category: "executor", Outcome: "blocked_external"},
		FinishedAt: "2026-11-20T09:30:30Z",
		ObservedAt: "2026-11-20T09:31:20Z",
	}}
	_, base := newContributeServer(t, secrets, stub)
	client := login(t, base, secrets.token)
	_, body := get(t, client, base+"/contributions/courier-3f2a")
	for _, want := range []string{"Failed", "executor", "blocked_external", "2026-11-20T09:30:30Z"} {
		if !strings.Contains(body, want) {
			t.Errorf("failed-run page missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "http-equiv=\"refresh\"") {
		t.Errorf("terminal run still self-refreshes:\n%s", body)
	}
}

func TestContributionDetailStateMessages(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"notfound", &ContributeStatusError{Status: http.StatusNotFound}, "reaped after it finished"},
		{"forbidden", &ContributeStatusError{Status: http.StatusForbidden}, "outside the namespaces"},
		{"denied", &ContributeStatusError{Status: http.StatusUnauthorized}, "refused this console&#39;s status credential"},
		{"busy", &ContributeStatusError{Status: http.StatusServiceUnavailable}, "at capacity"},
		{"mismatch", &ContributeStatusError{Status: http.StatusTeapot}, "unexpected status (418)"},
		{"unexpected", fmt.Errorf("%w: broken", ErrContributeStatusUnexpected), "not with a contribution status"},
		{"transport", errors.New("dial tcp: connect: connection refused"), "could not be reached"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			secrets := newTestSecrets(t)
			_, base := newContributeServer(t, secrets, &fakeContributeStatus{err: tc.err})
			client := login(t, base, secrets.token)
			resp, body := get(t, client, base+"/contributions/courier-3f2a")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 with a state message", resp.StatusCode)
			}
			if !strings.Contains(body, tc.want) {
				t.Errorf("state message missing %q:\n%s", tc.want, body)
			}
		})
	}
}

func TestContributionTransientFailureBacksOff(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newContributeServer(t, secrets, &fakeContributeStatus{err: &ContributeStatusError{Status: http.StatusServiceUnavailable}})
	client := login(t, base, secrets.token)
	_, body := get(t, client, base+"/contributions/courier-3f2a")
	if !strings.Contains(body, fmt.Sprintf("content=\"%d\"", contributeStatusBackoffSeconds)) {
		t.Errorf("transient failure did not set the backoff interval:\n%s", body)
	}
}

func TestContributionInvalidInputNeverReachesBackend(t *testing.T) {
	secrets := newTestSecrets(t)
	stub := &fakeContributeStatus{}
	_, base := newContributeServer(t, secrets, stub)
	client := login(t, base, secrets.token)
	resp, body := get(t, client, base+"/contributions/BADNAME")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "not a valid run name") {
		t.Fatalf("invalid run name = %d, missing the local rejection:\n%s", resp.StatusCode, body)
	}
	if _, body := get(t, client, base+"/contributions/courier-abc?namespace=bad_ns"); !strings.Contains(body, "not a valid namespace") {
		t.Fatalf("invalid namespace missing the local rejection:\n%s", body)
	}
	if len(stub.calls) != 0 {
		t.Errorf("invalid input reached the backend: %v", stub.calls)
	}
}

func TestContributionsIndexLookup(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newContributeServer(t, secrets, &fakeContributeStatus{status: fullContributionStatus()})
	client := login(t, base, secrets.token)
	resp, _ := get(t, client, base+"/contributions?run=courier-3f2a")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/contributions/courier-3f2a" {
		t.Fatalf("valid lookup = %d %q, want a redirect to the detail page", resp.StatusCode, resp.Header.Get("Location"))
	}
	if resp, _ := get(t, client, base+"/contributions?run=courier-3f2a&namespace=contrib"); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/contributions/courier-3f2a?namespace=contrib" {
		t.Fatalf("namespace-pinned lookup = %d %q, want a redirect that keeps the pin", resp.StatusCode, resp.Header.Get("Location"))
	}
	resp, body := get(t, client, base+"/contributions?run=Bad%20Name")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "Enter the exact run name") {
		t.Fatalf("invalid lookup = %d, missing the inline message:\n%s", resp.StatusCode, body)
	}
}

// --- history cookie -----------------------------------------------------------

// getWithHistory performs a GET carrying the client's session cookie plus an
// optional history cookie value. The test jar holds only one cookie, so a
// history cookie observed on a response must ride manually on the next
// request — which also keeps each case's cookie state explicit.
func getWithHistory(t *testing.T, client *http.Client, rawURL, historyValue string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	applyCookie(t, client, req)
	if historyValue != "" {
		req.AddCookie(&http.Cookie{Name: contributeHistoryCookie, Value: historyValue})
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp, readAll(t, resp)
}

func historyCookieFrom(t *testing.T, resp *http.Response) *http.Cookie {
	t.Helper()
	for _, c := range resp.Cookies() {
		if c.Name == contributeHistoryCookie {
			return c
		}
	}
	return nil
}

func signedHistoryValue(t *testing.T, s *Server, entries []contributeHistoryEntry) string {
	t.Helper()
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	return payload + "." + s.signContributeHistory(payload)
}

func TestContributeHistoryRoundTrip(t *testing.T) {
	secrets := newTestSecrets(t)
	server, base := newContributeServer(t, secrets, &fakeContributeStatus{status: fullContributionStatus()})
	client := login(t, base, secrets.token)
	resp, body := get(t, client, base+"/contributions/courier-3f2a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	_ = body
	history := historyCookieFrom(t, resp)
	if history == nil {
		t.Fatal("a successful status read did not store the recently-viewed cookie")
	}
	if !history.HttpOnly || history.SameSite != http.SameSiteLaxMode || !history.Secure {
		t.Errorf("history cookie flags = (HttpOnly %v, SameSite %v, Secure %v)", history.HttpOnly, history.SameSite, history.Secure)
	}
	payload, sig, found := strings.Cut(history.Value, ".")
	if !found {
		t.Fatalf("cookie value %q is not payload.signature", history.Value)
	}
	if got := server.signContributeHistory(payload); got != sig {
		t.Error("history cookie signature does not verify under the session key")
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}
	var entries []contributeHistoryEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Run != "courier-3f2a" || entries[0].Namespace != "contrib" {
		t.Fatalf("history entries = %+v, want the viewed contribution", entries)
	}
	_, indexBody := getWithHistory(t, client, base+"/contributions", history.Value)
	if !strings.Contains(indexBody, `href="/contributions/courier-3f2a?namespace=contrib"`) {
		t.Errorf("index does not link the recent contribution:\n%s", indexBody)
	}
}

func TestContributeHistoryRejectsForgedCookie(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newContributeServer(t, secrets, &fakeContributeStatus{status: fullContributionStatus()})
	client := login(t, base, secrets.token)
	entries := []contributeHistoryEntry{{Namespace: "contrib", Run: "courier-evil"}}
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte("attacker-key"))
	mac.Write([]byte(contributeHistoryScope + payload))
	forged := payload + "." + hex.EncodeToString(mac.Sum(nil))
	_, body := getWithHistory(t, client, base+"/contributions", forged)
	if strings.Contains(body, "courier-evil") {
		t.Errorf("forged history cookie rendered:\n%s", body)
	}
}

func TestContributeHistoryCapsAndValidatesEntries(t *testing.T) {
	secrets := newTestSecrets(t)
	server, base := newContributeServer(t, secrets, &fakeContributeStatus{status: fullContributionStatus()})
	client := login(t, base, secrets.token)
	entries := make([]contributeHistoryEntry, 0, 12)
	for i := 0; i < 12; i++ {
		entries = append(entries, contributeHistoryEntry{Namespace: fmt.Sprintf("ns%d", i), Run: fmt.Sprintf("courier-%02d", i)})
	}
	value := signedHistoryValue(t, server, entries)
	_, body := getWithHistory(t, client, base+"/contributions", value)
	for i := 0; i < maxContributeHistoryEntries; i++ {
		if !strings.Contains(body, fmt.Sprintf("courier-%02d", i)) {
			t.Errorf("entry %d missing from the capped list:\n%s", i, body)
		}
	}
	for i := maxContributeHistoryEntries; i < 12; i++ {
		if strings.Contains(body, fmt.Sprintf("courier-%02d", i)) {
			t.Errorf("entry %d rendered past the cap:\n%s", i, body)
		}
	}
	// A correctly signed list carrying an invalid entry renders its valid
	// prefix only.
	mixed := []contributeHistoryEntry{
		{Namespace: "contrib", Run: "BAD_RUN"},
		{Namespace: "contrib", Run: "courier-good"},
	}
	_, body = getWithHistory(t, client, base+"/contributions", signedHistoryValue(t, server, mixed))
	if strings.Contains(body, "BAD_RUN") {
		t.Errorf("invalid entry rendered from a signed cookie:\n%s", body)
	}
	if !strings.Contains(body, "courier-good") {
		t.Errorf("valid entry dropped alongside an invalid one:\n%s", body)
	}
}

func TestContributionDetailPRLinkSanitized(t *testing.T) {
	hostile := fullContributionStatus()
	hostile.Publication.PR = "javascript:alert(1)"
	cases := []struct {
		name    string
		pr      string
		wantAbs string
	}{
		{"javascript", "javascript:alert(1)", `href="javascript`},
		{"http", "http://github.com/o/r/pull/1", `href="http://github.com`},
		{"credentials", "https://user:pw@github.com/o/r/pull/1", `href="https://user`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			secrets := newTestSecrets(t)
			status := hostile
			status.Publication = &contributionPublication{PR: tc.pr}
			_, base := newContributeServer(t, secrets, &fakeContributeStatus{status: status})
			client := login(t, base, secrets.token)
			_, body := get(t, client, base+"/contributions/courier-3f2a")
			if strings.Contains(body, tc.wantAbs) {
				t.Errorf("hostile PR value %q rendered as a link:\n%s", tc.pr, body)
			}
			// The value still renders, as escaped text, so the operator can
			// see what the run recorded.
			if !strings.Contains(body, tc.pr) {
				t.Errorf("PR value %q missing from the page text:\n%s", tc.pr, body)
			}
		})
	}
}

func TestContributionForgeLinks(t *testing.T) {
	const pr = "https://github.com/misospace/courier/pull/42"
	if got := contributionRepoLink(pr, "contributor/fork"); got != "https://github.com/contributor/fork" {
		t.Errorf("repo link = %q, want the PR URL's host with the owner/name path", got)
	}
	if got := contributionCommitLink(pr, "0f4a9c8b"); got != pr+"/commit/0f4a9c8b" {
		t.Errorf("commit link = %q, want the commit path under the PR URL", got)
	}
	// A query or fragment on the vetted PR URL never carries into a derived
	// link, not even an empty forced query.
	if got := contributionCommitLink("https://ghe.example/o/r/pull/1?x=1#frag", "0f4a9c8b"); got != "https://ghe.example/o/r/pull/1/commit/0f4a9c8b" {
		t.Errorf("derived link carried the query or fragment: %q", got)
	}
	if got := contributionRepoLink("https://ghe.example/o/r/pull/1?", "misospace/courier"); got != "https://ghe.example/misospace/courier" {
		t.Errorf("derived link carried a forced empty query: %q", got)
	}
	// With no vetted PR URL nothing derives: the projected values stay text.
	for _, link := range []string{contributionRepoLink("", "misospace/courier"), contributionCommitLink("", "0f4a9c8b")} {
		if link != "" {
			t.Errorf("a derived link existed without a vetted PR URL: %q", link)
		}
	}
	for _, repo := range []string{"", "solo", "a/b/c", "bad name", "bad?x=1", "https://evil.example/o", "user:pw@evil.example/o", "/escape", "./x", "../x", "o/.", "o/..", strings.Repeat("a", 300) + "/b"} {
		if got := contributionRepoLink(pr, repo); got != "" {
			t.Errorf("contributionRepoLink(%q) = %q, want rejected", repo, got)
		}
	}
	if got := contributionRepoLink(pr, "dot./ok"); got != "https://github.com/dot./ok" {
		t.Errorf("a dot-suffixed-but-not-dot-only segment should still link, got %q", got)
	}
	for _, sha := range []string{"", "xyz", "0f4a9c", "0f4a9c8b../", "0f4a9c8b?x=1", "0f4a9c8b z", strings.Repeat("0", 65)} {
		if got := contributionCommitLink(pr, sha); got != "" {
			t.Errorf("contributionCommitLink(%q) = %q, want rejected", sha, got)
		}
	}
}

// TestContributionDetailWrongShape200RendersMisconfigured pins the handler's
// re-check of the decode boundary: a status that does not describe the
// requested run — say what a bare {} decodes to, or another run's echo —
// renders the documented misconfiguration state. It must not render found
// state with blank fields, keep an active refresh, or store a history entry.
// The stub stands in for the client, so these rows pin the handler-side
// describes gate: a namespace echo that fights the request's pin, a missing
// echo, and a stamp that is present but no stamp the clock understands.
func TestContributionDetailWrongShape200RendersMisconfigured(t *testing.T) {
	cases := map[string]struct {
		status contributionStatus
		path   string
	}{
		"zero body":        {},
		"other run":        {status: contributionStatus{Run: "some-other-run", ObservedAt: "2026-11-20T09:00:00Z"}},
		"missing readtime": {status: contributionStatus{Run: "courier-3f2a", Phase: "Pending"}},
		"unparseable readtime": {status: contributionStatus{
			Run: "courier-3f2a", Namespace: "contrib", ObservedAt: "last tuesday",
		}},
		"empty namespace echo": {status: contributionStatus{
			Run: "courier-3f2a", ObservedAt: "2026-11-20T09:00:00Z",
		}},
		"pinned namespace mismatch": {
			status: contributionStatus{
				Run: "courier-3f2a", Namespace: "elsewhere", ObservedAt: "2026-11-20T09:00:00Z",
			},
			path: "/contributions/courier-3f2a?namespace=contrib",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			secrets := newTestSecrets(t)
			_, base := newContributeServer(t, secrets, &fakeContributeStatus{status: tc.status})
			client := login(t, base, secrets.token)
			path := tc.path
			if path == "" {
				path = "/contributions/courier-3f2a"
			}
			resp, body := get(t, client, base+path)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 with a state message", resp.StatusCode)
			}
			if !strings.Contains(body, "not with a contribution status") {
				t.Errorf("wrong-shape 200 did not render the misconfiguration message:\n%s", body)
			}
			if strings.Contains(body, "Run identity") || strings.Contains(body, "Published</h2>") {
				t.Errorf("wrong-shape 200 rendered found-state sections:\n%s", body)
			}
			if strings.Contains(body, `http-equiv="refresh"`) {
				t.Errorf("misconfiguration is a static state and must not self-refresh:\n%s", body)
			}
			for _, c := range resp.Cookies() {
				if c.Name == contributeHistoryCookie {
					t.Errorf("wrong-shape 200 stored a history entry: %+v", c)
				}
			}
		})
	}
}

func TestRunExplorerLinksAdhocRowsWithNamespace(t *testing.T) {
	const readAPIToken = "RT-adhoc-4d71"
	body := runListJSON(t, 0, 50, 2, false, false,
		runItemJSON(t, map[string]any{"name": "run-adhoc", "namespace": "team-a", "phase": "Running", "source": "adhoc", "lane": "ci"}),
		runItemJSON(t, map[string]any{"name": "run-other", "namespace": "team-a", "phase": "Running", "source": "github", "lane": "ci"}),
	)
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, body, capture)
	defer backend.Close()

	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, readAPIToken)
	client := login(t, base, secrets.token)
	_, htmlBody := get(t, client, base+"/runs")
	if !strings.Contains(htmlBody, `href="/contributions/run-adhoc?namespace=team-a"`) {
		t.Errorf("adhoc row missing its namespaced status link:\n%s", htmlBody)
	}
	if strings.Contains(htmlBody, "/contributions/run-other") {
		t.Errorf("non-adhoc row linked to the contribution status page:\n%s", htmlBody)
	}
}

func TestContributeHistoryTailStaysWithinCookieBudget(t *testing.T) {
	secrets := newTestSecrets(t)
	server, base := newContributeServer(t, secrets, &fakeContributeStatus{status: fullContributionStatus()})
	client := login(t, base, secrets.token)
	long := strings.Repeat("r", 240)
	entries := make([]contributeHistoryEntry, 0, 8)
	for i := 0; i < 8; i++ {
		entries = append(entries, contributeHistoryEntry{Namespace: strings.Repeat("n", 63), Run: fmt.Sprintf("%s-%02d", long, i)})
	}
	value := signedHistoryValue(t, server, entries)
	resp, _ := getWithHistory(t, client, base+"/contributions/courier-3f2a", value)
	var written *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == contributeHistoryCookie {
			written = c
		}
	}
	if written == nil {
		t.Fatal("no history cookie written")
	}
	if len(written.Value) > maxContributeHistoryCookieBytes {
		t.Fatalf("written history cookie is %d bytes, past the %d budget", len(written.Value), maxContributeHistoryCookieBytes)
	}
	_, indexBody := getWithHistory(t, client, base+"/contributions", written.Value)
	if !strings.Contains(indexBody, "courier-3f2a") {
		t.Errorf("the just-viewed contribution was evicted before the long tail:\n%s", indexBody)
	}
}

func TestContributeHistoryHeaderModeHasNoCookie(t *testing.T) {
	server, err := NewServer(Config{Mode: AuthModeHeader, TrustedHeader: "X-Test-Identity"})
	if err != nil {
		t.Fatal(err)
	}
	server.contributions = &fakeContributeStatus{status: fullContributionStatus()}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/contributions/courier-3f2a", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Test-Identity", "operator")
	resp, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("header-mode status page = %d, want 200", resp.StatusCode)
	}
	for _, c := range resp.Cookies() {
		if c.Name == contributeHistoryCookie {
			t.Fatal("header mode minted a history cookie without a session key")
		}
	}
}

func TestContributeHistoryClearedOnLogout(t *testing.T) {
	secrets := newTestSecrets(t)
	_, base := newContributeServer(t, secrets, &fakeContributeStatus{status: fullContributionStatus()})
	client := login(t, base, secrets.token)
	resp, body := get(t, client, base+"/contributions/courier-3f2a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	csrf := findCSRFToken(t, body)
	if csrf == "" {
		t.Fatal("no CSRF token on the page")
	}
	logout, err := http.NewRequest(http.MethodPost, base+"/logout", strings.NewReader("csrf="+csrf))
	if err != nil {
		t.Fatal(err)
	}
	logout.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	applyCookie(t, client, logout)
	resp, err = client.Do(logout)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var cleared *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == contributeHistoryCookie {
			cleared = c
		}
	}
	if cleared == nil || cleared.Value != "" || cleared.MaxAge >= 0 {
		t.Fatalf("logout did not clear the history cookie: %+v", cleared)
	}
}

// TestContributeSubmitThenTrackStatus is the integrated #830 fixture: one
// fake backend serves both the admission POST and the status GET, and one
// console is wired to both. An accepted submission links its run's status
// page pinned to the admission namespace; a repeat submission of unchanged
// content resumes the same run and namespace and links the same page; and
// following that link tracks the run from admitted through verifying to a
// published pull request — the operator's whole path from submission to PR
// without leaving the console. The correlation is the admission response
// the console already holds; the status DTO never carries a submission key.
func TestContributeSubmitThenTrackStatus(t *testing.T) {
	const (
		admittedRun = "courier-3f2a"
		admittedNS  = "courier-contrib"
	)
	var mu sync.Mutex
	var submissions int
	phase := "Pending"
	publication := ""
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/contributions":
			mu.Lock()
			submissions++
			repeat := submissions > 1
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if repeat {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusCreated)
			}
			_, _ = fmt.Fprintf(w, `{"run":%q,"namespace":%q,"workItemId":"item-7"}`, admittedRun, admittedNS)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/contributions/"):
			if got := r.URL.Query().Get("namespace"); got != admittedNS {
				t.Errorf("status read namespace = %q, want the pinned admission namespace %q", got, admittedNS)
			}
			mu.Lock()
			body := fmt.Sprintf(`{"run":%q,"namespace":%q,"phase":%q,"observedAt":%q`,
				admittedRun, admittedNS, phase, time.Now().UTC().Format(time.RFC3339))
			if publication != "" {
				body += fmt.Sprintf(`,"publication":{"pr":%q}`, publication)
			}
			body += "}"
			mu.Unlock()
			_, _ = w.Write([]byte(body))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(backend.Close)

	secrets := newTestSecrets(t)
	server, err := NewServer(Config{
		Mode:              AuthModeToken,
		SessionKey:        []byte(secrets.sessionKey),
		OperatorTokens:    [][]byte{[]byte(secrets.token)},
		ContributeURL:     backend.URL,
		ContributeToken:   []byte("admission-secret"),
		ContributeTimeout: time.Second,
		InsecureCookies:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.contributions, err = newHTTPContributeStatusClient(backend.URL, []byte("status-secret"))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	client := login(t, httpServer.URL, secrets.token)
	base := httpServer.URL

	statusHref := `href="/contributions/` + admittedRun + `?namespace=` + admittedNS + `"`
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	resp, page := postContribution(t, client, base, csrf, contributionValues("first goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was created") {
		t.Fatalf("first submission: status = %d, page:\n%s", resp.StatusCode, page)
	}
	if !strings.Contains(page, statusHref) {
		t.Fatalf("accepted submission missing the namespace-pinned status link:\n%s", page)
	}

	resp, page = postContribution(t, client, base, csrf, contributionValues("first goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was resumed") {
		t.Fatalf("repeat submission: status = %d, page:\n%s", resp.StatusCode, page)
	}
	if !strings.Contains(page, statusHref) {
		t.Fatalf("resumed submission missing the same namespace-pinned status link:\n%s", page)
	}

	_, detail := get(t, client, base+"/contributions/"+admittedRun+"?namespace="+admittedNS)
	for _, want := range []string{"Admitted, waiting to start", admittedNS} {
		if !strings.Contains(detail, want) {
			t.Errorf("admitted status page missing %q:\n%s", want, detail)
		}
	}
	if strings.Contains(detail, "This read is stale") {
		t.Errorf("a fresh fixture read renders the stale label:\n%s", detail)
	}

	mu.Lock()
	phase = "Verifying"
	mu.Unlock()
	_, detail = get(t, client, base+"/contributions/"+admittedRun+"?namespace="+admittedNS)
	if !strings.Contains(detail, "Verifying") || strings.Contains(detail, "Admitted, waiting to start") {
		t.Errorf("verifying progression:\n%s", detail)
	}

	mu.Lock()
	phase = "AwaitingReview"
	publication = "https://github.com/owner/base/pull/42"
	mu.Unlock()
	_, detail = get(t, client, base+"/contributions/"+admittedRun+"?namespace="+admittedNS)
	if !strings.Contains(detail, `href="https://github.com/owner/base/pull/42"`) || !strings.Contains(detail, "Awaiting review") {
		t.Errorf("published progression:\n%s", detail)
	}
}
