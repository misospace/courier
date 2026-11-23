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
	if firstKey != secondKey {
		t.Fatal("identical resubmission of a settled attempt did not replay the same intent key")
	}
	if intentCookie(client) == nil {
		t.Fatal("submit response lost the intent cookie delivered before the admission call")
	}
	// A later page view signs the retained cookie as settled and drops the
	// stale unsettled-attempt notice without removing the browser's key.
	settledPage, page := get(t, client, base+"/contribute")
	settledCookie := intentCookie(client)
	if settledCookie == nil {
		t.Fatal("page view after settlement cleared the retained intent cookie")
	}
	if parts := strings.Split(settledCookie.Value, "."); len(parts) != 4 || parts[2] != "1" {
		t.Fatalf("page view did not rewrite cookie with settled flag: %q", settledCookie.Value)
	}
	for _, cookie := range settledPage.Cookies() {
		if cookie.Name == contributeIntentCookie && (cookie.MaxAge < 0 || cookie.Value != settledCookie.Value) {
			t.Fatalf("settled page expired or failed to rewrite cookie: %+v", cookie)
		}
	}
	if strings.Contains(page, "unsettled attempt") {
		t.Fatal("page view after settlement still shows the unsettled-attempt notice")
	}
	// The browser-held settled flag preserves replay across process restarts; the
	// minter mapping also preserves replay after a newer attempt overwrites it.
	resp, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was resumed") {
		t.Fatalf("post-clearing resubmission response: %s", page)
	}
	mu.Lock()
	thirdKey := bodies[3]["idempotencyKey"].(string)
	mu.Unlock()
	if firstKey != thirdKey {
		t.Fatal("identical resubmission after the settled cookie was cleared did not replay the same intent key")
	}
}

func TestContributeSettledCookieAllowsDifferentContent(t *testing.T) {
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		keys = append(keys, body.IdempotencyKey)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
	}))
	defer api.Close()
	_, base, client := newContributionTestServer(t, api.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	postContribution(t, client, base, csrf, contributionValues("first"))
	get(t, client, base+"/contribute")
	cookie := intentCookie(client)
	if cookie == nil || strings.Split(cookie.Value, ".")[2] != "1" {
		t.Fatalf("settled cookie missing flag: %+v", cookie)
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("different"))
	if strings.Contains(page, "unsettled attempt") || len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("settled cookie blocked different content or reused key: page=%s keys=%v", page, keys)
	}
}

func TestContributeSettledCookieSurvivesRestart(t *testing.T) {
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		keys = append(keys, body.IdempotencyKey)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
	}))
	defer api.Close()
	secrets := newTestSecrets(t)
	config := Config{Mode: AuthModeToken, SessionKey: []byte(secrets.sessionKey), OperatorTokens: [][]byte{[]byte(secrets.token)}, ContributeURL: api.URL, ContributeToken: []byte("admission-secret"), ContributeTimeout: time.Second, InsecureCookies: true}
	firstServer, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	firstHTTP := httptest.NewServer(firstServer.Handler())
	defer firstHTTP.Close()
	client := login(t, firstHTTP.URL, secrets.token)
	_, page := get(t, client, firstHTTP.URL+"/contribute")
	csrf := findCSRFToken(t, page)
	postContribution(t, client, firstHTTP.URL, csrf, contributionValues("goal"))
	get(t, client, firstHTTP.URL+"/contribute")
	settledCookie := intentCookie(client)
	if settledCookie == nil || strings.Split(settledCookie.Value, ".")[2] != "1" {
		t.Fatalf("settled cookie missing flag: %+v", settledCookie)
	}
	originalKey := keys[0]

	freshServer, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	freshHTTP := httptest.NewServer(freshServer.Handler())
	defer freshHTTP.Close()
	freshClient := login(t, freshHTTP.URL, secrets.token)
	freshURL, err := url.Parse(freshHTTP.URL)
	if err != nil {
		t.Fatal(err)
	}
	freshClient.Jar.SetCookies(freshURL, []*http.Cookie{settledCookie})
	_, page = get(t, freshClient, freshHTTP.URL+"/contribute")
	csrf = findCSRFToken(t, page)
	postContribution(t, freshClient, freshHTTP.URL, csrf, contributionValues("goal"))
	if len(keys) != 2 || keys[1] != originalKey {
		t.Fatalf("fresh server did not replay settled key: %v", keys)
	}
	// The settled flag itself survived the restart, so different content is
	// not blocked the way an unsettled attempt would block it.
	_, page = postContribution(t, freshClient, freshHTTP.URL, csrf, contributionValues("different"))
	if strings.Contains(page, "unsettled attempt") {
		t.Fatalf("settled flag wrongly blocked different content after restart: %s", page)
	}
	if len(keys) != 3 || keys[2] == originalKey {
		t.Fatalf("different content did not mint a fresh key: %v", keys)
	}
}

func TestContributeRestartDegradedResubmitHeals(t *testing.T) {
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		keys = append(keys, body.IdempotencyKey)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
	}))
	defer api.Close()
	secrets := newTestSecrets(t)
	config := Config{Mode: AuthModeToken, SessionKey: []byte(secrets.sessionKey), OperatorTokens: [][]byte{[]byte(secrets.token)}, ContributeURL: api.URL, ContributeToken: []byte("admission-secret"), ContributeTimeout: time.Second, InsecureCookies: true}
	firstServer, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	firstHTTP := httptest.NewServer(firstServer.Handler())
	defer firstHTTP.Close()
	client := login(t, firstHTTP.URL, secrets.token)
	_, page := get(t, client, firstHTTP.URL+"/contribute")
	csrf := findCSRFToken(t, page)
	postContribution(t, client, firstHTTP.URL, csrf, contributionValues("goal"))

	// A restart before the next page view loses the settlement record, so
	// the settled attempt looks unsettled: the notice reappears, but the
	// form stays submittable and resubmitting the content unchanged replays
	// the admitted key and re-settles the attempt.
	freshServer, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	freshHTTP := httptest.NewServer(freshServer.Handler())
	defer freshHTTP.Close()
	freshClient := login(t, freshHTTP.URL, secrets.token)
	freshURL, err := url.Parse(freshHTTP.URL)
	if err != nil {
		t.Fatal(err)
	}
	if cookie := intentCookie(client); cookie != nil {
		freshClient.Jar.SetCookies(freshURL, []*http.Cookie{cookie})
	}
	_, page = get(t, freshClient, freshHTTP.URL+"/contribute")
	if !strings.Contains(page, "unsettled attempt") {
		t.Fatalf("post-restart page omitted the stale unsettled notice: %s", page)
	}
	csrf = findCSRFToken(t, page)
	postContribution(t, freshClient, freshHTTP.URL, csrf, contributionValues("goal"))
	if len(keys) != 2 || keys[1] != keys[0] {
		t.Fatalf("degraded resubmission did not replay the settled key: %v", keys)
	}
	_, page = get(t, freshClient, freshHTTP.URL+"/contribute")
	if strings.Contains(page, "unsettled attempt") {
		t.Fatalf("resubmit did not re-settle the degraded attempt: %s", page)
	}
	if cookie := intentCookie(freshClient); cookie == nil || strings.Split(cookie.Value, ".")[2] != "1" {
		t.Fatalf("healed page did not rewrite the settled flag: %+v", cookie)
	}
}

func TestContributeUnsettledAbandonMintsFreshKey(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	admission := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		mu.Lock()
		keys = append(keys, body["idempotencyKey"].(string))
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer admission.Close()
	_, base, client := newContributionTestServer(t, admission.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	resp, page := postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("uncertain response: %s", page)
	}
	// The deliberate abandon of an unsettled attempt frees its digest: the
	// discarded key cannot describe a later resubmission, so a fresh key is
	// minted and the abandoned attempt may still have been admitted.
	resp, err := client.PostForm(base+"/contribute/new", url.Values{"csrf": {csrf}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("new response = %d", resp.StatusCode)
	}
	resp, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("post-abandon resubmission response: %s", page)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("abandon did not mint a fresh key: %v", keys)
	}
}

func TestContributeAbandonSettledStartsNewAttempt(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	admission := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		mu.Lock()
		keys = append(keys, body["idempotencyKey"].(string))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
	}))
	defer admission.Close()
	_, base, client := newContributionTestServer(t, admission.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	resp, page := postContribution(t, client, base, csrf, contributionValues("goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was created") {
		t.Fatalf("created response: %s", page)
	}
	// No page view intervenes, so the cookie still carries the unsettled
	// flag; the server-side settlement record is the only settled truth.
	if cookie := intentCookie(client); cookie == nil || strings.Split(cookie.Value, ".")[2] != "0" {
		t.Fatalf("pre-abandon cookie unexpectedly settled: %+v", cookie)
	}
	// Retrying the settled attempt without an explicit action replays its
	// key, so an accidental identical resubmission cannot duplicate the run.
	resp, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was created") {
		t.Fatalf("settled retry response: %s", page)
	}
	// The explicit "start a new contribution" action ends the attempt even
	// when settled — straight off the success page, before any page view
	// signs the settled flag: it frees this session's key slot, so an
	// identical resubmission mints a fresh key and requests genuinely new
	// work. That is what makes a deliberate new attempt distinguishable
	// from a retry of a settled one.
	resp, err := client.PostForm(base+"/contribute/new", url.Values{"csrf": {csrf}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("new response = %d", resp.StatusCode)
	}
	if cookie := intentCookie(client); cookie != nil && cookie.Value != "" {
		t.Fatalf("abandon did not clear the cookie: %+v", cookie)
	}
	resp, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "A run was created") {
		t.Fatalf("post-abandon resubmission response: %s", page)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 3 || keys[1] != keys[0] {
		t.Fatalf("settled retry did not replay the settled key: %v", keys)
	}
	if keys[2] == keys[0] {
		t.Fatalf("explicit new attempt after settle did not mint a fresh key: %v", keys)
	}
}

// TestContributeIdenticalContentDistinctSessions is the reviewer's
// two-browser scenario with no minter.forget anywhere: two authenticated
// sessions submit the same payload, and each must reach admission with its
// own idempotency key — never the other session's, so admission can never
// resume one session's run for the other's independent submission.
func TestContributeIdenticalContentDistinctSessions(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		keys = append(keys, body.IdempotencyKey)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
	}))
	defer api.Close()
	secrets := newTestSecrets(t)
	server, err := NewServer(Config{Mode: AuthModeToken, SessionKey: []byte(secrets.sessionKey), OperatorTokens: [][]byte{[]byte(secrets.token)}, ContributeURL: api.URL, ContributeToken: []byte("admission-secret"), ContributeTimeout: time.Second, InsecureCookies: true})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	clientA := login(t, httpServer.URL, secrets.token)
	_, page := get(t, clientA, httpServer.URL+"/contribute")
	csrfA := findCSRFToken(t, page)
	if _, page = postContribution(t, clientA, httpServer.URL, csrfA, contributionValues("goal")); !strings.Contains(page, "A run was created") {
		t.Fatalf("session A created state missing: %s", page)
	}
	// Session B — separate login, no intent cookie — submits the identical
	// payload: an independent attempt that must mint its own key.
	clientB := login(t, httpServer.URL, secrets.token)
	_, page = get(t, clientB, httpServer.URL+"/contribute")
	csrfB := findCSRFToken(t, page)
	if _, page = postContribution(t, clientB, httpServer.URL, csrfB, contributionValues("goal")); !strings.Contains(page, "A run was created") {
		t.Fatalf("session B created state missing: %s", page)
	}
	// Each session's unchanged resubmission replays its own key, and only
	// its own.
	if _, page = postContribution(t, clientB, httpServer.URL, csrfB, contributionValues("goal")); !strings.Contains(page, "A run was created") {
		t.Fatalf("session B retry state missing: %s", page)
	}
	if _, page = postContribution(t, clientA, httpServer.URL, csrfA, contributionValues("goal")); !strings.Contains(page, "A run was created") {
		t.Fatalf("session A retry state missing: %s", page)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 4 {
		t.Fatalf("keys = %v, want four admission calls", keys)
	}
	if keys[1] == keys[0] {
		t.Fatalf("session B reused session A's key: %v", keys)
	}
	if keys[2] != keys[1] {
		t.Fatalf("session B retry did not replay session B's key: %v", keys)
	}
	if keys[3] != keys[0] {
		t.Fatalf("session A retry did not replay session A's key: %v", keys)
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
		t.Fatalf("GET after uncertain outcome omitted notice: %s", page)
	}
	// The form stays submittable: re-entering the earlier content unchanged
	// is the retry path, and the server refuses changed content before any
	// admission call.
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("retry keys = %v", keys)
	}
	changed := contributionValues("changed goal")
	_, page = postContribution(t, client, base, csrf, changed)
	if !strings.Contains(page, "unsettled attempt") {
		t.Fatalf("changed-content submission was not blocked: %s", page)
	}
	if len(keys) != 2 {
		t.Fatalf("changed content reached the admission API: %v", keys)
	}
	// The blocked submission must not overwrite the earlier still-unsettled
	// attempt's cookie and must not mint a key that lives nowhere durable.
	intentParts := strings.Split(intentCookie(client).Value, ".")
	if len(intentParts) != 4 || intentParts[0] != keys[0] || intentParts[1] != contributeDigest(contributeSubmission{Lane: "default", BaseRepo: "owner/base", BaseRef: "main", WorkRepo: "owner/fork", Goal: "goal"}) || intentParts[2] != "0" {
		t.Fatalf("blocked submission overwrote the earlier unsettled intent cookie: %v", intentParts)
	}
	status = http.StatusBadRequest
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "busy") {
		t.Fatalf("refusal message missing: %s", page)
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if keys[2] != keys[0] || keys[3] != keys[0] {
		t.Fatalf("refused outcome lost the settled key mapping: %v", keys)
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
	key, _, _, ok := verifyContributeIntent(intentCookie(client).Value, server.auth.key)
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
	fresh, _, _, ok := verifyContributeIntent(intentCookie(client).Value, server.auth.key)
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
			if intentCookie(client) == nil {
				t.Fatal("definitive outcome lost the intent cookie delivered before the admission call")
			}
			// A later page view marks the retained cookie settled without expiring it.
			response, _ := get(t, client, base+"/contribute")
			cookie := intentCookie(client)
			if cookie == nil {
				t.Fatal("page view after definitive outcome cleared the retained intent cookie")
			}
			parts := strings.Split(cookie.Value, ".")
			if len(parts) != 4 || parts[2] != "1" {
				t.Fatalf("settled page did not mark cookie settled: %q", cookie.Value)
			}
			for _, setCookie := range response.Cookies() {
				if setCookie.Name == contributeIntentCookie && setCookie.MaxAge < 0 {
					t.Fatalf("settled page expired intent cookie: %+v", setCookie)
				}
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
	if intentCookie(client) == nil {
		t.Fatal("unreachable outcome lost the intent cookie delivered before the admission call")
	}
	response, _ := get(t, client, base+"/contribute")
	cookie := intentCookie(client)
	if cookie == nil || strings.Split(cookie.Value, ".")[2] != "1" {
		t.Fatal("page view after unreachable outcome did not mark the retained cookie settled")
	}
	for _, setCookie := range response.Cookies() {
		if setCookie.Name == contributeIntentCookie && setCookie.MaxAge < 0 {
			t.Fatal("page view after unreachable outcome expired the retained intent cookie")
		}
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

func TestContributeIntentDeliveredBeforeAdmissionResult(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		keys = append(keys, body.IdempotencyKey)
		first := len(keys) == 1
		mu.Unlock()
		if first {
			// Hang until the test client abandons the request, so its
			// response is lost exactly as a dead console or dropped
			// connection would lose it.
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer api.Close()
	server, base, client := newContributionTestServer(t, api.URL, 5*time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	values := contributionValues("goal")
	values.Set("csrf", csrf)
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/contribute", strings.NewReader(values.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()
	// Wait for the handler's pre-admission flush to reach the jar: that is
	// the durable-delivery point this test pins down.
	deadline := time.Now().Add(5 * time.Second)
	for intentCookie(client) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	// Whether the client saw a transport error or an empty truncated page,
	// the response is lost from the operator's perspective; the signed
	// intent must already be durable in the browser: resubmitting replays
	// the same key instead of minting a new one that could duplicate an
	// admitted run.
	cookie := intentCookie(client)
	if cookie == nil {
		t.Fatal("lost response orphaned the attempt: intent cookie was never delivered")
	}
	storedKey, digest, settled, ok := verifyContributeIntent(cookie.Value, server.auth.key)
	mu.Lock()
	firstKey, keyCount := "", len(keys)
	if keyCount > 0 {
		firstKey = keys[0]
	}
	mu.Unlock()
	if keyCount == 0 {
		t.Fatal("admission API never saw the abandoned submission")
	}
	if !ok || settled || storedKey != firstKey || digest != contributeDigest(contributeSubmission{Lane: "default", BaseRepo: "owner/base", BaseRef: "main", WorkRepo: "owner/fork", Goal: "goal"}) {
		t.Fatalf("delivered intent invalid: key=%q digest=%q ok=%v", storedKey, digest, ok)
	}
	_, page = get(t, client, base+"/contribute")
	csrf = findCSRFToken(t, page)
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("retry state missing: %s", page)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[1] != storedKey {
		t.Fatalf("retry keys = %v, want the delivered key replayed", keys)
	}
}

func TestContributeUncertainReplayKeepsSettledKeyRetryable(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	status := http.StatusCreated
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		keys = append(keys, body.IdempotencyKey)
		current := status
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(current)
		if current == http.StatusCreated {
			_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
			return
		}
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer api.Close()
	_, base, client := newContributionTestServer(t, api.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "A run was created") {
		t.Fatalf("created state missing: %s", page)
	}
	mu.Lock()
	status = http.StatusServiceUnavailable
	mu.Unlock()
	// An identical resubmission replays the settled key and turns uncertain:
	// the attempt is no longer settled, so a later page view must keep the
	// cookie and its retry key, not clear them.
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("uncertain state missing: %s", page)
	}
	_, page = get(t, client, base+"/contribute")
	if intentCookie(client) == nil {
		t.Fatal("page view after an uncertain replay cleared the retry intent cookie")
	}
	if !strings.Contains(page, "unsettled attempt") {
		t.Fatal("page view after an uncertain replay lost the unsettled notice")
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("second retry state missing: %s", page)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 3 || keys[1] != keys[0] || keys[2] != keys[0] {
		t.Fatalf("keys = %v, want one key replayed throughout", keys)
	}
}

func TestContributeCrossSessionSettlementKeepsUnsettledIntent(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	status := http.StatusServiceUnavailable
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		keys = append(keys, body.IdempotencyKey)
		current := status
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(current)
		if current == http.StatusCreated {
			_, _ = w.Write([]byte(`{"run":"run-1","namespace":"courier","workItemId":"item-1"}`))
			return
		}
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer api.Close()
	secrets := newTestSecrets(t)
	server, err := NewServer(Config{Mode: AuthModeToken, SessionKey: []byte(secrets.sessionKey), OperatorTokens: [][]byte{[]byte(secrets.token)}, ContributeURL: api.URL, ContributeToken: []byte("admission-secret"), ContributeTimeout: time.Second, InsecureCookies: true})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	clientA := login(t, httpServer.URL, secrets.token)
	_, page := get(t, clientA, httpServer.URL+"/contribute")
	csrfA := findCSRFToken(t, page)
	_, page = postContribution(t, clientA, httpServer.URL, csrfA, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("session A uncertain state missing: %s", page)
	}
	cookieA := intentCookie(clientA)
	if cookieA == nil {
		t.Fatal("session A uncertain outcome wrote no intent cookie")
	}
	// Session B settles the same content with its own key: the minter slot
	// is session-scoped, so B needs no server-side clearing to be an
	// independent submission — this is the production cross-session path.
	clientB := login(t, httpServer.URL, secrets.token)
	_, page = get(t, clientB, httpServer.URL+"/contribute")
	csrfB := findCSRFToken(t, page)
	mu.Lock()
	status = http.StatusCreated
	mu.Unlock()
	_, page = postContribution(t, clientB, httpServer.URL, csrfB, contributionValues("goal"))
	if !strings.Contains(page, "A run was created") {
		t.Fatalf("session B created state missing: %s", page)
	}
	// Session A submits different content: B's settlement of the shared
	// content must not authorize overwriting A's still-unsettled cookie, so
	// the submission is refused before any admission call.
	mu.Lock()
	status = http.StatusServiceUnavailable
	mu.Unlock()
	_, page = postContribution(t, clientA, httpServer.URL, csrfA, contributionValues("goal two"))
	if !strings.Contains(page, "unsettled attempt") {
		t.Fatalf("session A other-content submission was not blocked: %s", page)
	}
	if got := intentCookie(clientA); got == nil || got.Value != cookieA.Value {
		t.Fatal("another session's settlement let a submission overwrite this session's unsettled intent cookie")
	}
	_, page = postContribution(t, clientA, httpServer.URL, csrfA, contributionValues("goal"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("session A retry state missing: %s", page)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 3 || keys[2] != keys[0] {
		t.Fatalf("keys = %v, want the cross-session unsettled key replayed", keys)
	}
	if keys[1] == keys[0] {
		t.Fatalf("session B inherited session A's key: %v", keys)
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
	jar.mu.Lock()
	defer jar.mu.Unlock()
	cookie := jar.cookies[contributeIntentCookie]
	if cookie == nil {
		return nil
	}
	copy := *cookie
	return &copy
}

func TestContributeIntentCookieVerification(t *testing.T) {
	key := []byte("session-key-material")
	value := contributeIntentValue("adhoc-console-0123456789abcdef", "0123456789abcdef", false, key)
	if got, digest, settled, ok := verifyContributeIntent(value, key); !ok || settled || got != "adhoc-console-0123456789abcdef" || digest != "0123456789abcdef" {
		t.Fatalf("verifyContributeIntent(valid) = %q, %q, %v, %v", got, digest, settled, ok)
	}
	settledValue := contributeIntentValue("adhoc-console-0123456789abcdef", "0123456789abcdef", true, key)
	if _, _, settled, ok := verifyContributeIntent(settledValue, key); !ok || !settled {
		t.Fatalf("verifyContributeIntent(settled) = %v, %v", settled, ok)
	}
	parts := strings.Split(value, ".")
	badFlag := append([]string(nil), parts...)
	badFlag[2] = "2"
	tamperedFlag := strings.Join(badFlag, ".")
	for name, tc := range map[string]struct {
		value string
		key   []byte
	}{
		"forged signature":  {value[:len(value)-2] + "zz", key},
		"wrong session key": {value, []byte("other-key")},
		"empty session key": {value, nil},
		"malformed shape":   {"adhoc-console-short.abc.def", key},
		"truncated fields":  {"adhoc-console-0123456789abcdef.abc." + value[strings.LastIndex(value, ".")+1:], key},
		"wrong part count":  {strings.Join(parts[:3], "."), key},
		"bad flag":          {tamperedFlag, key},
		"tampered flag":     {strings.Join([]string{parts[0], parts[1], "1", parts[3]}, "."), key},
		"garbage":           {"nonsense", key},
	} {
		if _, _, _, ok := verifyContributeIntent(tc.value, tc.key); ok {
			t.Fatalf("%s: verifyContributeIntent accepted an invalid cookie", name)
		}
	}
}

func TestContributeMinter(t *testing.T) {
	var m contributeMinter
	now := time.Unix(0, 0)
	first, err := m.keyFor("session-a", "digest-a", now)
	if err != nil {
		t.Fatal(err)
	}
	again, err := m.keyFor("session-a", "digest-a", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatal("minter minted a second key for the same digest inside the TTL")
	}
	// Another session's identical content is an independent submission:
	// it must mint its own key, never inherit this session's.
	other, err := m.keyFor("session-b", "digest-a", now)
	if err != nil {
		t.Fatal(err)
	}
	if other == first {
		t.Fatal("minter shared a key across sessions for identical content")
	}
	m.forget("session-a", "digest-a")
	fresh, err := m.keyFor("session-a", "digest-a", now.Add(90*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if fresh == first {
		t.Fatal("minter resurrected a forgotten digest")
	}
	if fresh == other {
		t.Fatal("forgotten session resurrected another session's key")
	}
	// An entry past its TTL is swept, not served: the re-minted entry
	// from 90s lives until 3m30s, so check past it.
	expired, err := m.keyFor("session-a", "digest-a", now.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if expired == fresh {
		t.Fatal("minter served an expired entry")
	}
}

func TestContributeDigestFieldBoundaries(t *testing.T) {
	base := contributeSubmission{Lane: "default", BaseRepo: "owner/base", BaseRef: "main", WorkRepo: "owner/fork", Goal: "goal"}
	digests := map[string]bool{contributeDigest(base): true}
	for _, variant := range []contributeSubmission{
		{Lane: "default\x00owner/base", BaseRepo: "base", BaseRef: "main", WorkRepo: "owner/fork", Goal: "goal"},
		{Lane: "default", BaseRepo: "owner/base\x00main", BaseRef: "main", WorkRepo: "owner/fork", Goal: "goal"},
		{Lane: "default", BaseRepo: "owner/base", BaseRef: "main", WorkRepo: "owner/fork", Goal: "goal\x00"},
	} {
		d := contributeDigest(variant)
		if digests[d] {
			t.Fatalf("digest collision for variant %+v", variant)
		}
		digests[d] = true
	}
}

func TestContributeIntentCookieFlags(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
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
	cookie := intentCookie(client)
	if cookie == nil {
		t.Fatal("uncertain outcome wrote no intent cookie")
	}
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode || cookie.Secure || cookie.Path != "/" || cookie.MaxAge <= 0 {
		t.Fatalf("intent cookie posture wrong: %+v", cookie)
	}
	// Every form-visible state carries the explicit new-contribution action.
	if !strings.Contains(page, "Start a new contribution") {
		t.Fatal("uncertain page missing the new-contribution action")
	}
}

func TestContributeOtherContentBlockedWhileUnsettledIntent(t *testing.T) {
	status := http.StatusServiceUnavailable
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"no"}`))
	}))
	defer api.Close()
	_, base, client := newContributionTestServer(t, api.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	_, page = postContribution(t, client, base, csrf, contributionValues("goal one"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("first attempt not uncertain: %s", page)
	}
	kept := intentCookie(client)
	if kept == nil {
		t.Fatal("uncertain outcome lost the intent cookie")
	}
	status = http.StatusBadRequest
	resp, page := postContribution(t, client, base, csrf, contributionValues("goal two"))
	if !strings.Contains(page, "unsettled attempt") || !strings.Contains(page, "Start a new contribution") {
		t.Fatalf("other content was not blocked: %s", page)
	}
	if calls != 1 {
		t.Fatalf("blocked submission reached the admission API: %d calls", calls)
	}
	for _, c := range resp.Cookies() {
		if c.Name == contributeIntentCookie {
			t.Fatalf("blocked response rewrote the intent cookie: %v", c)
		}
	}
	after := intentCookie(client)
	if after == nil || after.Value != kept.Value {
		t.Fatal("a blocked submission for other content destroyed the unsettled intent")
	}
	_, page = get(t, client, base+"/contribute")
	if !strings.Contains(page, "unsettled attempt") {
		t.Fatal("later GET lost the unsettled notice")
	}
	// Resubmitting the earlier content replays its key.
	keys := map[string]int{}
	recording := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		keys[body.IdempotencyKey]++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer recording.Close()
	_, base2, client2 := newContributionTestServer(t, recording.URL, time.Second)
	_, page = get(t, client2, base2+"/contribute")
	csrf = findCSRFToken(t, page)
	postContribution(t, client2, base2, csrf, contributionValues("goal one"))
	first := intentCookie(client2)
	_, _ = postContribution(t, client2, base2, csrf, contributionValues("goal one"))
	second := intentCookie(client2)
	if first == nil || second == nil || first.Value != second.Value || keys[first.Value[:len("adhoc-console-")+16]] != 2 {
		t.Fatal("identical-content retry did not replay the same key")
	}
}

func TestContributeBlockedAbandonThenNewContent(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		keys = append(keys, body.IdempotencyKey)
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer api.Close()
	server, base, client := newContributionTestServer(t, api.URL, time.Second)
	_, page := get(t, client, base+"/contribute")
	csrf := findCSRFToken(t, page)
	_, page = postContribution(t, client, base, csrf, contributionValues("goal one"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("first attempt not uncertain: %s", page)
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal two"))
	if !strings.Contains(page, "unsettled attempt") {
		t.Fatalf("other content was not blocked: %s", page)
	}
	server.minter.mu.Lock()
	minterEntries := len(server.minter.entries)
	server.minter.mu.Unlock()
	if minterEntries != 1 {
		t.Fatalf("blocked submission touched the minter: %d entries", minterEntries)
	}
	// The only way past the block is the explicit abandon, which frees the
	// browser's durable key slot so new content can own it.
	resp, err := client.PostForm(base+"/contribute/new", url.Values{"csrf": {csrf}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("abandon response = %d", resp.StatusCode)
	}
	_, page = postContribution(t, client, base, csrf, contributionValues("goal two"))
	if !strings.Contains(page, "may have been admitted") {
		t.Fatalf("post-abandon new content was not admitted to a new attempt: %s", page)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[1] == keys[0] {
		t.Fatalf("post-abandon keys = %v; want a fresh key for the new content", keys)
	}
}
