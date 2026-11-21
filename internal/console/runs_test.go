package console

import (
	"encoding/json"
	"fmt"
	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/readapi"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// stubCapture records what the console asked the (stubbed) read API so a test
// can pin the path, method, bearer token, and query the explorer emits.
type stubCapture struct {
	method string
	path   string
	auth   string
	query  url.Values
	calls  int
}

func newStubReadAPI(t *testing.T, status int, body string, capture *stubCapture) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if capture != nil {
			capture.method = r.Method
			capture.path = r.URL.Path
			capture.auth = r.Header.Get("Authorization")
			capture.query = r.URL.Query()
			capture.calls++
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
}

func runItemJSON(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func runListJSON(t *testing.T, page, pageSize, total int, more, truncated bool, items ...string) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString(`{"apiVersion":"courier.misospace.dev/v1","kind":"RunSummaryList","payload":{"items":[`)
	sb.WriteString(strings.Join(items, ","))
	fmt.Fprintf(&sb, `],"pagination":{"page":%d,"pageSize":%d,"total":%d,"more":%t,"truncated":%t}}}`,
		page, pageSize, total, more, truncated)
	return sb.String()
}

func i64(v int64) *int64 { return &v }

// TestRunExplorerRendersRows covers the happy path: a healthy read API page
// renders each run as a stable relative link with its projected fields, and
// the request to the backend is a token-bearing GET of the allowlisted path.
func TestRunExplorerRendersRows(t *testing.T) {
	const readAPIToken = "RT-live-9f2c"
	body := runListJSON(t, 0, 50, 3, false, false,
		runItemJSON(t, map[string]any{"name": "run-alpha", "namespace": "ns", "phase": "Running", "source": "github", "lane": "ci", "prNumber": 123, "restarts": 0, "terminal": false, "ageSeconds": 300}),
		runItemJSON(t, map[string]any{"name": "run-beta", "namespace": "ns", "phase": "Done", "source": "dispatch", "lane": "ci", "restarts": 2, "terminal": true, "ageSeconds": 3725}),
		// No ageSeconds: the age cell must read as unknown (em dash), never 0s.
		runItemJSON(t, map[string]any{"name": "run-omega", "namespace": "ns", "phase": "Pending", "source": "dispatch", "lane": "ci"}),
	)
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, body, capture)
	defer backend.Close()

	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, readAPIToken)
	client := login(t, base, secrets.token)

	resp, htmlBody := get(t, client, base+"/runs")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /runs status = %d, want 200", resp.StatusCode)
	}
	for _, want := range []string{
		`href="/runs/run-alpha"`, "run-alpha", ">Running<", ">github<", ">ci<",
		"#123", ">5m<", "run-beta", ">Done<", ">dispatch<", ">1h02m<", ">2</td>",
		"tag-terminal", "End of results",
	} {
		if !strings.Contains(htmlBody, want) {
			t.Fatalf("runs page missing %q:\n%s", want, htmlBody)
		}
	}
	// A run with no creation timestamp shows the unknown dash, not a zero age.
	if !strings.Contains(htmlBody, "run-omega") {
		t.Fatalf("third run missing:\n%s", htmlBody)
	}
	if strings.Count(htmlBody, ">—<") < 1 {
		t.Fatalf("nil-age run did not render the unknown dash:\n%s", htmlBody)
	}
	if capture.method != http.MethodGet || capture.path != "/v1/runs" {
		t.Fatalf("backend reached %s %s, want GET /v1/runs", capture.method, capture.path)
	}
	if capture.auth != "Bearer "+readAPIToken {
		t.Fatalf("backend Authorization = %q, want the bearer token", capture.auth)
	}
}

// TestRunExplorerPassesFiltersAndPreservesState pins that the validated filter
// reaches the backend as query parameters and is echoed back into the form.
func TestRunExplorerPassesFiltersAndPreservesState(t *testing.T) {
	body := runListJSON(t, 0, 10, 1, false, false,
		runItemJSON(t, map[string]any{"name": "only-run", "phase": "Running", "source": "gh", "lane": "ci"}),
	)
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, body, capture)
	defer backend.Close()

	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
	client := login(t, base, secrets.token)

	resp, htmlBody := get(t, client, base+"/runs?lane=ci&source=gh&phase=Running&pageSize=10")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	for key, want := range map[string]string{"lane": "ci", "source": "gh", "phase": "Running", "pageSize": "10"} {
		if got := capture.query.Get(key); got != want {
			t.Fatalf("backend query %s = %q, want %q", key, got, want)
		}
	}
	if capture.path != "/v1/runs" {
		t.Fatalf("path = %q, want /v1/runs (filters must stay in the query)", capture.path)
	}
	for _, want := range []string{
		`name="lane" value="ci"`, `name="source" value="gh"`,
		`value="Running" selected`, `value="10" selected`,
		`href="/runs">Clear filters`,
	} {
		if !strings.Contains(htmlBody, want) {
			t.Fatalf("form did not preserve %q:\n%s", want, htmlBody)
		}
	}
}

// TestRunExplorerPaginationLinksPreserveFilters pins that prev/next links keep
// the active filter and advance the page, and that the page number is 1-based.
func TestRunExplorerPaginationLinksPreserveFilters(t *testing.T) {
	body := runListJSON(t, 1, 50, 3, true, false,
		runItemJSON(t, map[string]any{"name": "mid-run", "phase": "Pending", "source": "gh", "lane": "ci"}),
	)
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, body, capture)
	defer backend.Close()

	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
	client := login(t, base, secrets.token)

	_, htmlBody := get(t, client, base+"/runs?page=1&lane=ci")
	if !strings.Contains(htmlBody, `href="/runs?lane=ci"`) {
		t.Fatalf("prev link missing or did not drop page=0:\n%s", htmlBody)
	}
	if !strings.Contains(htmlBody, `href="/runs?lane=ci&amp;page=2"`) {
		t.Fatalf("next link missing or did not preserve the filter:\n%s", htmlBody)
	}
	if !strings.Contains(htmlBody, "page 2") {
		t.Fatalf("page number is not 1-based:\n%s", htmlBody)
	}
	if !strings.Contains(htmlBody, "More runs follow") {
		t.Fatalf("more-available note missing:\n%s", htmlBody)
	}
}

// TestRunExplorerPaginationPreservesPageSize pins that the page-size filter
// survives across prev/next links, so paging never silently resets the page
// size back to the default.
func TestRunExplorerPaginationPreservesPageSize(t *testing.T) {
	body := runListJSON(t, 1, 25, 30, true, false,
		runItemJSON(t, map[string]any{"name": "r", "phase": "Pending", "source": "gh", "lane": "ci"}),
	)
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, body, capture)
	defer backend.Close()

	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
	client := login(t, base, secrets.token)

	_, htmlBody := get(t, client, base+"/runs?page=1&lane=ci&pageSize=25")
	if got := capture.query.Get("pageSize"); got != "25" {
		t.Fatalf("backend pageSize = %q, want 25", got)
	}
	if !strings.Contains(htmlBody, `href="/runs?lane=ci&amp;page=2&amp;pageSize=25"`) {
		t.Fatalf("next link did not preserve pageSize:\n%s", htmlBody)
	}
	if !strings.Contains(htmlBody, `href="/runs?lane=ci&amp;pageSize=25"`) {
		t.Fatalf("prev link did not preserve pageSize:\n%s", htmlBody)
	}
	// The dropdown reflects the active 25.
	if !strings.Contains(htmlBody, `value="25" selected`) {
		t.Fatalf("dropdown did not reflect the active page size:\n%s", htmlBody)
	}
}

// TestRunExplorerPageNumberFromRequest pins that the page number and pager come
// from the request, not the backend echo: even if a middlebox echoes a
// different page, the displayed page and links track what the operator asked
// for.
func TestRunExplorerPageNumberFromRequest(t *testing.T) {
	// Operator is on page 2 (page=1); a middlebox echoes page=7.
	body := runListJSON(t, 7, 50, 5, true, false,
		runItemJSON(t, map[string]any{"name": "r", "phase": "Pending", "source": "gh", "lane": "ci"}),
	)
	backend := newStubReadAPI(t, http.StatusOK, body, nil)
	defer backend.Close()
	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
	client := login(t, base, secrets.token)

	_, htmlBody := get(t, client, base+"/runs?page=1")
	if !strings.Contains(htmlBody, "page 2") {
		t.Fatalf("page number should come from the request (page 2):\n%s", htmlBody)
	}
	// Next must advance to page 3 (page=2), never follow the echoed page 7.
	if !strings.Contains(htmlBody, `href="/runs?page=2"`) {
		t.Fatalf("next link should be page=2, not derived from the echoed page:\n%s", htmlBody)
	}
	if strings.Contains(htmlBody, "page=8") || strings.Contains(htmlBody, "page=7") {
		t.Fatalf("link leaked the echoed page number:\n%s", htmlBody)
	}
}

// TestRunExplorerPageOverMaxRejected pins the page ceiling is enforced before
// any backend call, so a crafted huge offset never reaches the projection.
func TestRunExplorerPageOverMaxRejected(t *testing.T) {
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, runListJSON(t, 0, 50, 0, false, false), capture)
	defer backend.Close()
	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
	client := login(t, base, secrets.token)

	_, htmlBody := get(t, client, base+"/runs?page=1000001")
	if !strings.Contains(htmlBody, "page number must be") {
		t.Fatalf("over-max page not rejected:\n%s", htmlBody)
	}
	if capture.calls != 0 {
		t.Fatalf("backend reached for an out-of-range page, want 0 calls")
	}
}

// TestHandlerKeepsAllowlistPath drives a hostile filter value through the HTTP
// handler (not just the client) and pins that the backend request path is still
// the fixed projection path, with the value carried only in the query.
func TestHandlerKeepsAllowlistPath(t *testing.T) {
	capture := &stubCapture{}
	backend := newStubReadAPI(t, http.StatusOK, runListJSON(t, 0, 50, 0, false, false), capture)
	defer backend.Close()
	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
	client := login(t, base, secrets.token)

	// A directory-traversal and a scheme-shaped value must stay query state.
	resp, _ := get(t, client, base+"/runs?lane="+url.QueryEscape("../../etc/passwd")+"&source="+url.QueryEscape("javascript:alert(1)")+"&phase=Running")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if capture.path != "/v1/runs" || capture.method != http.MethodGet {
		t.Fatalf("handler steered the backend to %s %s, want GET /v1/runs", capture.method, capture.path)
	}
	if got := capture.query.Get("lane"); got != "../../etc/passwd" {
		t.Fatalf("lane filter = %q, want it carried verbatim in the query only", got)
	}
	if got := capture.query.Get("source"); got != "javascript:alert(1)" {
		t.Fatalf("source filter = %q, want it carried verbatim in the query only", got)
	}
}

// TestRunExplorerInvalidFiltersRejectedWithoutBackend pins that invalid filter
// state renders a message and never reaches the backend.
func TestRunExplorerInvalidFiltersRejectedWithoutBackend(t *testing.T) {
	cases := []struct {
		name, query, want string
	}{
		{"unknown phase", "/runs?phase=Bogus", "phase filter must be one of the listed"},
		{"oversized page size", "/runs?pageSize=999", "page size must be between 1 and 200"},
		{"zero page size", "/runs?pageSize=0", "page size must be between 1 and 200"},
		{"non-numeric page", "/runs?page=abc", "page number must be"},
		{"negative page", "/runs?page=-3", "page number must be"},
		{"over-long lane", "/runs?lane=" + strings.Repeat("x", 300), "at most 253 characters"},
		{"control char in source", "/runs?source=a%00b", "control characters"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := &stubCapture{}
			backend := newStubReadAPI(t, http.StatusOK, runListJSON(t, 0, 50, 0, false, false), capture)
			defer backend.Close()
			secrets := newTestSecrets(t)
			_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
			client := login(t, base, secrets.token)

			resp, htmlBody := get(t, client, base+tc.query)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if !strings.Contains(htmlBody, tc.want) {
				t.Fatalf("body missing rejection %q:\n%s", tc.want, htmlBody)
			}
			if capture.calls != 0 {
				t.Fatalf("backend was called %d times for an invalid filter, want 0", capture.calls)
			}
		})
	}
}

// TestRunExplorerErrorStates pins that an outage, a refused token, a busy read
// API, and a missing token each render a distinct message instead of an empty
// namespace.
func TestRunExplorerErrorStates(t *testing.T) {
	t.Run("401 refused token", func(t *testing.T) {
		backend := newStubReadAPI(t, http.StatusUnauthorized, `{"error":"the read API requires a valid bearer token"}`, nil)
		defer backend.Close()
		secrets := newTestSecrets(t)
		_, base := newTestServerWithToken(t, secrets, backend.URL, "wrong-token")
		client := login(t, base, secrets.token)
		_, body := get(t, client, base+"/runs")
		if !strings.Contains(body, "refused this console") {
			t.Fatalf("401 body missing refused-token state:\n%s", body)
		}
		if strings.Contains(body, "valid bearer token") {
			t.Fatal("the console echoed the read API's response body")
		}
	})
	t.Run("503 busy", func(t *testing.T) {
		backend := newStubReadAPI(t, http.StatusServiceUnavailable, `{"error":"busy"}`, nil)
		defer backend.Close()
		secrets := newTestSecrets(t)
		_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
		client := login(t, base, secrets.token)
		_, body := get(t, client, base+"/runs")
		if !strings.Contains(body, "read budget or past its own deadline") {
			t.Fatalf("503 body missing busy state:\n%s", body)
		}
	})
	t.Run("no token configured", func(t *testing.T) {
		capture := &stubCapture{}
		backend := newStubReadAPI(t, http.StatusOK, runListJSON(t, 0, 50, 0, false, false), capture)
		defer backend.Close()
		secrets := newTestSecrets(t)
		// Backend URL set but no bearer token: the explorer must say so and
		// never make the request.
		_, base := newTestServer(t, secrets, backend.URL)
		client := login(t, base, secrets.token)
		_, body := get(t, client, base+"/runs")
		if !strings.Contains(body, "no token") {
			t.Fatalf("missing-token body missing the state:\n%s", body)
		}
		if capture.calls != 0 {
			t.Fatalf("backend reached without a token, want 0 calls")
		}
	})
	t.Run("unreachable read API", func(t *testing.T) {
		secrets := newTestSecrets(t)
		// A backend URL that is immediately closed: the transport errors.
		closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		addr := closed.URL
		closed.Close()
		_, base := newTestServerWithToken(t, secrets, addr, "tok")
		client := login(t, base, secrets.token)
		_, body := get(t, client, base+"/runs")
		if !strings.Contains(body, "could not be reached") {
			t.Fatalf("transport-failure body missing the unreachable state:\n%s", body)
		}
	})
}

// TestRunExplorerEmptyAndTruncated pins that an empty page is not an outage and
// that a truncated window is stated.
func TestRunExplorerEmptyAndTruncated(t *testing.T) {
	secrets := newTestSecrets(t)

	empty := newStubReadAPI(t, http.StatusOK, runListJSON(t, 0, 50, 0, false, false), nil)
	defer empty.Close()
	_, base := newTestServerWithToken(t, secrets, empty.URL, "tok")
	client := login(t, base, secrets.token)
	_, body := get(t, client, base+"/runs")
	if !strings.Contains(body, "No runs matched") {
		t.Fatalf("empty page should say no runs, not an error:\n%s", body)
	}

	trunc := newStubReadAPI(t, http.StatusOK,
		runListJSON(t, 0, 50, 1, false, true,
			runItemJSON(t, map[string]any{"name": "r", "phase": "Pending", "source": "gh", "lane": "ci"})), nil)
	defer trunc.Close()
	_, truncBase := newTestServerWithToken(t, secrets, trunc.URL, "tok")
	truncClient := login(t, truncBase, secrets.token)
	_, body = get(t, truncClient, truncBase+"/runs")
	if !strings.Contains(body, "truncated window") {
		t.Fatalf("truncated page should state the cap:\n%s", body)
	}
}

// TestRunExplorerEscapesUntrustedProjectionFields pins that model- and
// repo-authored strings crossing the read API cannot inject markup.
func TestRunExplorerEscapesUntrustedProjectionFields(t *testing.T) {
	hostile := map[string]string{
		"name":   "run<script>alert(1)</script>",
		"source": `<img src=x onerror=alert(1)>`,
		"lane":   `"><b>bold`,
		"phase":  "Pending",
	}
	body := runListJSON(t, 0, 50, 1, false, false, runItemJSON(t, map[string]any{
		"name": hostile["name"], "phase": hostile["phase"], "source": hostile["source"], "lane": hostile["lane"],
	}))
	backend := newStubReadAPI(t, http.StatusOK, body, nil)
	defer backend.Close()

	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
	client := login(t, base, secrets.token)
	resp, htmlBody := get(t, client, base+"/runs")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// No raw tag may be parsed. (An escaped attribute fragment like a literal
	// "onerror=" is inert text, not markup, so it is not on this list; the
	// live "<img"/"<script"/"<b>" tag forms are.)
	for _, forbidden := range []string{"<script>", "<img src=x", "<b>bold"} {
		if strings.Contains(htmlBody, forbidden) {
			t.Fatalf("response contains raw markup %q", forbidden)
		}
	}
	// The escaped forms are present, proving the value was rendered, not dropped.
	for _, want := range []string{"&lt;script&gt;", "&lt;img src=x", "&lt;b&gt;bold"} {
		if !strings.Contains(htmlBody, want) {
			t.Fatalf("response missing escaped form %q:\n%s", want, htmlBody)
		}
	}
}

// TestRunExplorerNeverLeaksTheReadAPIToken covers (i) for the new surface: the
// bearer token never appears in the rendered explorer page.
func TestRunExplorerNeverLeaksTheReadAPIToken(t *testing.T) {
	const readAPIToken = "READAPI-TOKEN-MARKER-4d1f"
	body := runListJSON(t, 0, 50, 1, false, false,
		runItemJSON(t, map[string]any{"name": "run-a", "phase": "Running", "source": "gh", "lane": "ci"}))
	backend := newStubReadAPI(t, http.StatusOK, body, nil)
	defer backend.Close()

	secrets := newTestSecrets(t)
	_, base := newTestServerWithToken(t, secrets, backend.URL, readAPIToken)
	client := login(t, base, secrets.token)
	for _, path := range []string{"/runs", "/runs?lane=ci", "/runs?page=1"} {
		_, htmlBody := get(t, client, base+path)
		if strings.Contains(htmlBody, readAPIToken) {
			t.Fatalf("GET %s leaked the read API bearer token", path)
		}
	}
}

// TestBackendRunsContract pins the backend client directly: only the
// allowlisted projection path, the bearer token header, query-encoded filters
// (a hostile filter value never changes the path), and the distinct
// not-configured / no-token sentinels.
func TestBackendRunsContract(t *testing.T) {
	capture := &stubCapture{}
	server := newStubReadAPI(t, http.StatusOK, runListJSON(t, 0, 50, 0, false, false), capture)
	defer server.Close()

	backend := NewBackend(server.URL, []byte("tok-abc"), time.Second)
	if _, err := backend.Runs(t.Context(), RunsFilter{Lane: "../../etc/passwd", Phase: "Running", Page: 2, PageSize: 10}); err != nil {
		t.Fatalf("Runs on a healthy backend: %v", err)
	}
	if capture.method != http.MethodGet || capture.path != "/v1/runs" {
		t.Fatalf("Runs reached %s %s, want GET /v1/runs", capture.method, capture.path)
	}
	if capture.auth != "Bearer tok-abc" {
		t.Fatalf("Authorization = %q, want the bearer token", capture.auth)
	}
	if got := capture.query.Get("lane"); got != "../../etc/passwd" {
		t.Fatalf("lane filter = %q, want it carried verbatim in the query", got)
	}
	if got := capture.query.Get("page"); got != "2" {
		t.Fatalf("page = %q, want 2", got)
	}

	if _, err := NewBackend("", nil, time.Second).Runs(t.Context(), RunsFilter{}); err != ErrBackendUnavailable {
		t.Fatalf("Runs without a backend URL = %v, want ErrBackendUnavailable", err)
	}
	if _, err := NewBackend(server.URL, nil, time.Second).Runs(t.Context(), RunsFilter{}); err != ErrBackendTokenMissing {
		t.Fatalf("Runs without a token = %v, want ErrBackendTokenMissing", err)
	}
}

// TestPhaseFilterOptionsAreValidReadAPIPhases guards the console's local phase
// list against drift from the read API's accepted enum: every option the
// explorer offers must be a phase the read API accepts as a filter.
func TestPhaseFilterOptionsAreValidReadAPIPhases(t *testing.T) {
	for _, phase := range phaseFilterOptions {
		if !readapi.ValidPhase(courierv1alpha1.Phase(phase)) {
			t.Errorf("phase filter option %q is rejected by readapi.ValidPhase", phase)
		}
	}
	if readapi.ValidPhase(courierv1alpha1.Phase("Bogus")) {
		t.Fatal("readapi.ValidPhase accepted a bogus phase; the guard is vacuous")
	}
}

// TestHumanDuration pins the coarse age rendering, including the explicit
// "unknown" dash for a missing timestamp.
func TestHumanDuration(t *testing.T) {
	cases := []struct {
		in   *int64
		want string
	}{
		{nil, "—"},
		{i64(0), "0s"},
		{i64(45), "45s"},
		{i64(300), "5m"},
		{i64(3725), "1h02m"},
		{i64(90000), "1d"},
	}
	for _, tc := range cases {
		if got := humanDuration(tc.in); got != tc.want {
			t.Errorf("humanDuration(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRunExplorerUnexpectedResponseIsNotEmpty pins that a 200 whose body is not
// a run list (wrong kind, or unparseable) is an outage, not an empty namespace,
// and that the body never echoes.
func TestRunExplorerUnexpectedResponseIsNotEmpty(t *testing.T) {
	secret := "OVERVIEW-PAYLOAD-MARKER-7c31"
	wrongKind := `{"apiVersion":"courier.misospace.dev/v1","kind":"Overview","payload":{"marker":"` + secret + `"}}`
	cases := map[string]string{
		"wrong kind":      wrongKind,
		"not json at all": `<html>proxied login page</html>` + secret,
		"json error body": `{"error":"internal error"}`,
		"empty object":    `{}`,
		"json null":       `null`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			backend := newStubReadAPI(t, http.StatusOK, body, nil)
			defer backend.Close()
			secrets := newTestSecrets(t)
			_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
			client := login(t, base, secrets.token)
			_, htmlBody := get(t, client, base+"/runs")
			if !strings.Contains(htmlBody, "not with a run list") {
				t.Fatalf("unexpected 200 must state the bad response, got:\n%s", htmlBody)
			}
			if strings.Contains(htmlBody, "No runs matched") {
				t.Fatalf("unexpected 200 rendered as an empty namespace:\n%s", htmlBody)
			}
			if strings.Contains(htmlBody, secret) {
				t.Fatalf("the backend body leaked into the page")
			}
		})
	}
}

// TestRunExplorerStatusStates pins the 400 and default-status branches and that
// the backend response body never echoes on any of them.
func TestRunExplorerStatusStates(t *testing.T) {
	const bodyMarker = "READAPI-ERR-BODY-MARKER-2a77"
	cases := []struct {
		status int
		want   string
	}{
		{http.StatusBadRequest, "rejected the request"},
		{http.StatusInternalServerError, "unexpected status (500)"},
		{http.StatusTeapot, "unexpected status (418)"},
	}
	for _, tc := range cases {
		backend := newStubReadAPI(t, tc.status, `{"error":"`+bodyMarker+`"}`, nil)
		secrets := newTestSecrets(t)
		_, base := newTestServerWithToken(t, secrets, backend.URL, "tok")
		client := login(t, base, secrets.token)
		_, htmlBody := get(t, client, base+"/runs")
		if !strings.Contains(htmlBody, tc.want) {
			t.Errorf("status %d: missing %q:\n%s", tc.status, tc.want, htmlBody)
		}
		if strings.Contains(htmlBody, bodyMarker) {
			t.Errorf("status %d: backend body echoed", tc.status)
		}
		backend.Close()
	}
}

// TestExplorerBoundsMirrorReadAPI guards the console's local numeric bounds and
// the envelope kind against drift from the read API package it deliberately
// does not import.
func TestExplorerBoundsMirrorReadAPI(t *testing.T) {
	if maxExplorerPageSize != readapi.MaxPageSize {
		t.Errorf("maxExplorerPageSize = %d, want readapi.MaxPageSize %d", maxExplorerPageSize, readapi.MaxPageSize)
	}
	if defaultPageSize != readapi.DefaultPageSize {
		t.Errorf("defaultPageSize = %d, want readapi.DefaultPageSize %d", defaultPageSize, readapi.DefaultPageSize)
	}
	if kindRunList != readapi.KindRunList {
		t.Errorf("kindRunList = %q, want readapi.KindRunList %q", kindRunList, readapi.KindRunList)
	}
}
