package console

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestContributePageAndSubmit(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	status := http.StatusCreated
	admission := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admission-secret" {
			t.Errorf("Authorization = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		for _, forbidden := range []string{"namespace", "fresh", "debug"} {
			if _, ok := body[forbidden]; ok {
				t.Errorf("submission unexpectedly contains %q", forbidden)
			}
		}
		mu.Lock()
		bodies = append(bodies, body)
		current := status
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if current == http.StatusCreated {
			w.WriteHeader(current)
			_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
			return
		}
		if current == http.StatusOK {
			w.WriteHeader(current)
			_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1","resumed":true}`))
			return
		}
		w.WriteHeader(current)
		_, _ = w.Write([]byte(`{"error":"policy refused this contribution"}`))
	}))
	defer admission.Close()
	_, base, client := newContributionTestServer(t, admission.URL, time.Second)
	resp, page := get(t, client, base+"/contribute")
	if strings.Contains(page, "admission-secret") || strings.Contains(page, admission.URL) {
		t.Fatal("configured contribution URL or bearer token appeared in the page")
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, `name="csrf"`) || !strings.Contains(page, `href="/contribute" class="active"`) {
		t.Fatalf("configured page missing form/nav/CSRF: %s", page)
	}
	csrf := findCSRFToken(t, page)
	status = http.StatusBadRequest
	resp, page = postContribution(t, client, base, csrf, contributionValues("<script>alert(1)</script>"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "policy refused this contribution") {
		t.Fatalf("refusal response: %s", page)
	}
	if strings.Contains(page, "<script>") || !strings.Contains(page, "&lt;script&gt;") {
		t.Fatalf("unescaped goal: %s", page)
	}
	if strings.Contains(page, "admission-secret") || strings.Contains(page, admission.URL) {
		t.Fatal("configured contribution URL or bearer token appeared in the response")
	}
	status = http.StatusCreated
	resp, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was created") || !strings.Contains(page, `href="/runs/run-1"`) {
		t.Fatalf("created response: %s", page)
	}
	if strings.Contains(page, "unsettled attempt") {
		t.Fatal("settled success page still shows the unsettled-intent notice")
	}
	if strings.Contains(page, "admission-secret") || strings.Contains(page, admission.URL) {
		t.Fatal("configured contribution URL or bearer token appeared in the response")
	}
	mu.Lock()
	firstKey := bodies[1]["idempotencyKey"].(string)
	mu.Unlock()
	if !strings.HasPrefix(firstKey, "adhoc-console-") {
		t.Fatalf("key = %q", firstKey)
	}
	status = http.StatusOK
	resp, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was resumed") {
		t.Fatalf("resume response: %s", page)
	}
	mu.Lock()
	secondKey := bodies[2]["idempotencyKey"].(string)
	mu.Unlock()
	if firstKey == secondKey {
		t.Fatal("definitive outcome did not clear the intent")
	}
	if intentCookie(client) != nil {
		t.Fatal("intent cookie remained after success")
	}
}

func TestContributeUnconfiguredAndHeaderMode(t *testing.T) {
	secrets := newTestSecrets(t)
	server, err := NewServer(Config{Mode: AuthModeToken, SessionKey: []byte(secrets.sessionKey), OperatorTokens: [][]byte{[]byte(secrets.token)}})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	client := login(t, httpServer.URL, secrets.token)
	_, body := get(t, client, httpServer.URL+"/contribute")
	if !strings.Contains(body, "not configured") {
		t.Fatalf("state missing: %s", body)
	}
	resp, body := postContribution(t, client, httpServer.URL, findCSRFToken(t, body), contributionValues("goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "not configured") {
		t.Fatalf("unconfigured POST: %d %s", resp.StatusCode, body)
	}
	header, err := NewServer(Config{Mode: AuthModeHeader, TrustedHeader: "X-Operator"})
	if err != nil {
		t.Fatal(err)
	}
	headerServer := httptest.NewServer(header.Handler())
	defer headerServer.Close()
	req, _ := http.NewRequest(http.MethodGet, headerServer.URL+"/contribute", nil)
	req.Header.Set("X-Operator", "operator")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := readAll(t, response); !strings.Contains(got, "requires token-mode sign-in") {
		t.Fatalf("header-mode state missing: %s", got)
	}
	request, _ := http.NewRequest(http.MethodPost, headerServer.URL+"/contribute", strings.NewReader("lane=default"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("X-Operator", "operator")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := readAll(t, response); !strings.Contains(got, "requires token-mode sign-in") {
		t.Fatalf("header-mode POST state missing: %s", got)
	}
}

func TestContributeOutcomeRetryAndCSRF(t *testing.T) {
	status := http.StatusServiceUnavailable
	calls := 0
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		keys = append(keys, body["idempotencyKey"].(string))
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer api.Close()
	_, base, client := newContributionTestServer(t, api.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("uncertain state missing: %s", page)
	}
	_, page = get(t, client, base+"/contribute")
	if !strings.Contains(page, "unsettled attempt") {
		t.Fatalf("GET after uncertain outcome omitted unsettled notice: %s", page)
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("retry keys = %v", keys)
	}
	changed := contributionValues("changed goal")
	_, _ = postContribution(t, client, base, csrf, changed)
	if len(keys) != 3 || keys[2] == keys[1] {
		t.Fatalf("changed-content key = %v; want a new attempt", keys)
	}
	intentParts := strings.Split(intentCookie(client).Value, ".")
	if len(intentParts) != 3 || intentParts[0] != keys[2] || intentParts[1] != contributeDigest(contributeSubmission{Lane: "default", BaseRepo: "owner/base", BaseRef: "main", WorkRepo: "owner/fork", Goal: "changed goal"}) {
		t.Fatalf("changed-content intent cookie = %v", intentParts)
	}
	status = http.StatusBadRequest
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "busy") {
		t.Fatalf("refusal message missing: %s", page)
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if keys[2] == keys[3] {
		t.Fatal("refused outcome retained intent")
	}
	before := calls
	bad, _ := postContribution(t, client, base, "bad", contributionValues("goal"))
	if bad.StatusCode != http.StatusForbidden || calls != before {
		t.Fatalf("bad CSRF status/calls = %d/%d", bad.StatusCode, calls)
	}
}

func TestContributeLocalValidationAndNewIntent(t *testing.T) {
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusServiceUnavailable) }))
	defer api.Close()
	server, base, client := newContributionTestServer(t, api.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	_, page = postContribution(t, client, base, csrf, contributionValues(strings.Repeat("x", 16385)))
	if calls != 0 || !strings.Contains(page, "goal exceeds 16384 bytes") {
		t.Fatalf("oversize reached API or did not refuse")
	}
	escaped := contributionValues(strings.Repeat(`"`, 16384))
	before := calls
	_, page = postContribution(t, client, base, csrf, escaped)
	if calls != before || !strings.Contains(page, "too large after JSON encoding") || intentCookie(client) != nil {
		t.Fatal("oversized encoded body reached API or wrote an intent cookie")
	}
	values := contributionValues("goal")
	values.Del("lane")
	before = calls
	_, page = postContribution(t, client, base, csrf, values)
	if calls != before || !strings.Contains(page, "lane is required") {
		t.Fatalf("missing field reached API or did not refuse")
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	key, _, ok := verifyContributeIntent(intentCookie(client).Value, server.auth.key)
	if !ok {
		t.Fatal("uncertain outcome did not retain intent cookie")
	}
	resp, err := client.PostForm(base+"/contribute/new", url.Values{"csrf": {csrf}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/contribute" {
		t.Fatalf("new response = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if intentCookie(client) != nil {
		t.Fatal("new-contribution action retained intent cookie")
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	fresh, _, ok := verifyContributeIntent(intentCookie(client).Value, server.auth.key)
	if !ok || fresh == key || !strings.Contains(page, "may have been admitted") {
		t.Fatal("new contribution did not mint a fresh retry key")
	}
}

func TestNewHTTPContributeClientURLValidation(t *testing.T) {
	for _, invalid := range []string{"ftp://example.test", "http://user@example.test", "http://example.test?q=1", "http://example.test#fragment", "http:///path"} {
		if _, err := newHTTPContributeClient(invalid, nil, time.Second); err == nil {
			t.Errorf("newHTTPContributeClient(%q) succeeded", invalid)
		}
	}
	client, err := newHTTPContributeClient("https://example.test/", nil, time.Second)
	if err != nil || client.url != "https://example.test" {
		t.Fatalf("trailing slash URL = %q, %v", client.url, err)
	}
}

func TestContributeClassifications(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   contributeOutcome
	}{
		{201, outcomeCreated}, {200, outcomeResumed}, {400, outcomeRefused}, {403, outcomeRefused}, {409, outcomeRefused},
		{401, outcomeCredentialDenied}, {404, outcomeAPIMismatch}, {405, outcomeAPIMismatch}, {413, outcomeRefused}, {415, outcomeRefused}, {422, outcomeAPIMismatch}, {503, outcomeUncertain}, {500, outcomeUncertain},
	} {
		if got := classifyContributeResponse(tc.status, []byte(`{"run":"r","namespace":"n","workItemId":"i","error":"safe"}`)).Outcome; got != tc.want {
			t.Errorf("status %d outcome = %s, want %s", tc.status, got, tc.want)
		}
	}
	if !provablyUnreached(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}) {
		t.Fatal("dial-refused failure did not classify as provably unreachable")
	}
	if got := classifyContributeError(&url.Error{Op: "Post", URL: "http://example.test/v1/contributions", Err: context.DeadlineExceeded}).Outcome; got != outcomeUncertain {
		t.Fatalf("deadline classification = %s, want uncertain", got)
	}
	if got := classifyContributeError(&url.Error{Op: "Post", URL: "http://example.test/v1/contributions", Err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}}).Outcome; got != outcomeUnreachable {
		t.Fatalf("dial classification = %s, want unreachable", got)
	}
	if got := classifyContributeError(&url.Error{Op: "Post", URL: "http://example.test/v1/contributions", Err: &net.OpError{Op: "write", Err: syscall.ECONNREFUSED}}).Outcome; got != outcomeUncertain {
		t.Fatalf("write classification = %s, want uncertain", got)
	}
	if got := classifyContributeError(&url.Error{Op: "Post", URL: "http://example.test/v1/contributions", Err: &net.DNSError{Err: "no such host", Name: "example.test"}}).Outcome; got != outcomeUnreachable {
		t.Fatalf("DNS classification = %s, want unreachable", got)
	}
}

func TestContributeCredentialAndMismatchOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		wantText string
	}{
		{name: "credential denied", status: http.StatusUnauthorized, wantText: "credential was not accepted"},
		{name: "api mismatch", status: http.StatusNotFound, wantText: "does not serve the expected admission API"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":"detail"}`))
			}))
			defer api.Close()
			_, base, client := newContributionTestServer(t, api.URL, time.Second)
			_, page := get(t, client, base+"/contribute")
			csrf := findCSRFToken(t, page)
			_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
			if !strings.Contains(page, tc.wantText) {
				t.Fatalf("outcome state missing from page: %s", page)
			}
			if intentCookie(client) != nil {
				t.Fatal("definitive outcome retained the intent cookie")
			}
		})
	}
}

func TestContributeUnreachableAndDeadline(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // Nothing listens there anymore: a provably unreached dial.
	_, base, client := newContributionTestServer(t, deadURL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "could not be reached") {
		t.Fatalf("unreachable state missing: %s", page)
	}
	if intentCookie(client) != nil {
		t.Fatal("unreachable outcome retained the intent cookie")
	}

	// A server that accepts the connection and never answers maps to the
	// uncertain state and keeps the retry key: the request may have been
	// admitted. The handler blocks until the test releases it, so Close
	// can never wait on a client-disconnect cancellation racing the
	// client's own timeout.
	release := make(chan struct{})
	hanging := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); hanging.Close() })
	_, hangingBase, hangingClient := newContributionTestServer(t, hanging.URL, 200*time.Millisecond)
	_, page = get(t, hangingClient, hangingBase+"/contribute")
	csrf = findCSRFToken(t, page)
	_, page = postContribution(t, hangingClient, hangingBase, csrf, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("uncertain state missing: %s", page)
	}
	if intentCookie(hangingClient) == nil {
		t.Fatal("uncertain outcome dropped the retry intent cookie")
	}
}

func newContributionTestServer(t *testing.T, apiURL string, timeout time.Duration) (*Server, string, *http.Client) {
	t.Helper()
	secrets := newTestSecrets(t)
	server, err := NewServer(Config{Mode: AuthModeToken, SessionKey: []byte(secrets.sessionKey), OperatorTokens: [][]byte{[]byte(secrets.token)}, ContributeURL: apiURL, ContributeToken: []byte("admission-secret"), ContributeTimeout: timeout, InsecureCookies: true})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return server, httpServer.URL, login(t, httpServer.URL, secrets.token)
}

func contributionValues(goal string) url.Values {
	return url.Values{"lane": {"default"}, "baseRepo": {"owner/base"}, "baseRef": {"main"}, "workRepo": {"owner/fork"}, "goal": {goal}, "upstreamIssue": {""}}
}

func postContribution(t *testing.T, client *http.Client, base, csrf string, values url.Values) (*http.Response, string) {
	t.Helper()
	values.Set("csrf", csrf)
	resp, err := client.PostForm(base+"/contribute", values)
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, resp)
	resp.Body.Close()
	return resp, body
}

func intentCookie(client *http.Client) *http.Cookie {
	jar := client.Jar.(*singleCookieJar)
	return jar.cookies[contributeIntentCookie]
}
