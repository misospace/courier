package adhoc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testToken is the static bearer token every test server authenticates.
const testToken = "test-token-not-a-real-secret"

// mintedAPIKeyRe is the settled shape of a fresh-minted API key.
var mintedAPIKeyRe = regexp.MustCompile(`^adhoc-api-[0-9a-f]{16}$`)

// fakeAdmitter records the Admission each call received and returns a
// configurable result, so tests can assert both the HTTP behavior and
// exactly what the adapter would have seen.
type fakeAdmitter struct {
	admissions []Admission
	result     Admitted
	err        error
}

func (f *fakeAdmitter) Admit(_ context.Context, in Admission) (Admitted, error) {
	f.admissions = append(f.admissions, in)
	return f.result, f.err
}

func (f *fakeAdmitter) calls() int { return len(f.admissions) }

// deadlineAdmitter records whether the admission context it received
// carried a live deadline inside the handler's admission bound, so the
// deadline enforcement itself is pinned, not just the mapping of a fake
// DeadlineExceeded.
type deadlineAdmitter struct {
	saw bool
}

func (d *deadlineAdmitter) Admit(ctx context.Context, _ Admission) (Admitted, error) {
	deadline, ok := ctx.Deadline()
	d.saw = ok && time.Until(deadline) > 0 && time.Until(deadline) <= admissionTimeout
	return Admitted{RunName: "run-1", WorkItemID: "k"}, nil
}

// newTestServer builds a server around the given admitter. The default
// namespace and the ownership boundary are set so every test exercises
// the same wiring; tests that need a different boundary construct their
// own server.
func newTestServer(t *testing.T, admitter Admitter) *AdmissionServer {
	t.Helper()
	srv, err := NewAdmissionServer(AdmissionServerConfig{
		Admitter:         admitter,
		Token:            testToken,
		Bind:             ":0",
		DefaultNamespace: "courier",
		OwnedNamespaces:  []string{"courier", "contrib"},
	})
	if err != nil {
		t.Fatalf("construct admission server: %v", err)
	}
	return srv
}

// validRequestBody is the baseline well-formed request body; tests mutate
// a copy per case. It omits the namespace so the happy path exercises
// defaulting.
func validRequestBody() map[string]any {
	return map[string]any{
		"lane":     "local",
		"baseRepo": "upstream/project",
		"baseRef":  "main",
		"workRepo": "contributor/project",
		"goal":     "fix the login flow",
	}
}

// post builds one authenticated POST against the handler with the given
// body bytes and content type, so each test controls exactly what varies.
func post(handler http.Handler, body []byte, contentType, authorization string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, PathContributions, strings.NewReader(string(body)))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// postJSON serializes body as JSON and posts it with the valid token and
// content type.
func postJSON(t *testing.T, handler http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	return post(handler, raw, "application/json", "Bearer "+testToken)
}

// postRaw posts a raw body string, for malformed-JSON cases.
func postRaw(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	return post(handler, []byte(body), "application/json", "Bearer "+testToken)
}

// assertNoTokenSweep fails the test when the bearer token leaked into the
// response body or headers. Every response in every case goes through this
// check.
func assertNoTokenSweep(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if strings.Contains(rec.Body.String(), testToken) {
		t.Fatalf("response body contains the bearer token: %q", rec.Body.String())
	}
	for name, values := range rec.Header() {
		for _, value := range values {
			if strings.Contains(value, testToken) {
				t.Fatalf("response header %q contains the bearer token: %q", name, value)
			}
		}
	}
}

// assertError asserts the status code and the exact error message of a
// rejection, and that the token never leaked.
func assertError(t *testing.T, rec *httptest.ResponseRecorder, code int, msg string) {
	t.Helper()
	assertNoTokenSweep(t, rec)
	if rec.Code != code {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, code, rec.Body.String())
	}
	var got struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	if msg != "" && got.Error != msg {
		t.Fatalf("error = %q, want %q", got.Error, msg)
	}
}

func TestRouteRejectsWrongMethodAndPath(t *testing.T) {
	handler := newTestServer(t, &fakeAdmitter{}).Handler()

	req := httptest.NewRequest(http.MethodGet, PathContributions, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertError(t, rec, http.StatusMethodNotAllowed, "contribution admission accepts POST only")

	for _, path := range []string{"/v1/other", "/v1/contributions/", "/v1/contributions/x", "/"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+testToken)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("POST %s: status = %d, want 404 (body: %s)", path, rec.Code, rec.Body.String())
		}
		assertNoTokenSweep(t, rec)
	}
}

func TestAuthRejectsWithOneConstantMessage(t *testing.T) {
	admitter := &fakeAdmitter{}
	handler := newTestServer(t, admitter).Handler()

	for name, auth := range map[string]string{
		"missing":      "",
		"non-bearer":   "Basic " + testToken,
		"wrong token":  "Bearer wrong-token",
		"empty bearer": "Bearer ",
	} {
		body, err := json.Marshal(validRequestBody())
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rec := post(handler, body, "application/json", auth)
		// Every failure shape gets the identical constant message: the
		// response must not reveal which part of the credential failed.
		assertError(t, rec, http.StatusUnauthorized, "contribution admission requires a valid bearer token")
		if admitter.calls() != 0 {
			t.Fatalf("%s: admitter invoked on rejected request", name)
		}
	}
}

func TestAuthAcceptsCorrectToken(t *testing.T) {
	admitter := &fakeAdmitter{result: Admitted{RunName: "run-1", WorkItemID: "k", Resumed: false}}
	handler := newTestServer(t, admitter).Handler()

	rec := postJSON(t, handler, validRequestBody())
	assertNoTokenSweep(t, rec)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if admitter.calls() != 1 {
		t.Fatalf("admitter calls = %d, want 1", admitter.calls())
	}
}

func TestContentTypeRejected(t *testing.T) {
	admitter := &fakeAdmitter{}
	handler := newTestServer(t, admitter).Handler()

	body, _ := json.Marshal(validRequestBody())
	rec := post(handler, body, "text/plain", "Bearer "+testToken)
	assertError(t, rec, http.StatusUnsupportedMediaType, "contribution admission requires application/json")
	if admitter.calls() != 0 {
		t.Fatal("admitter invoked on rejected content type")
	}

	// Parameters are ignored; the bare type decides.
	rec = post(handler, body, "application/json; charset=utf-8", "Bearer "+testToken)
	if rec.Code != http.StatusCreated {
		t.Fatalf("application/json with parameters: status = %d, want 201", rec.Code)
	}
}

func TestBodyCapRejectsDeclaredAndChunkedOverflow(t *testing.T) {
	admitter := &fakeAdmitter{}
	handler := newTestServer(t, admitter).Handler()

	// Declared overflow: ContentLength above the cap is rejected before
	// the body is read.
	big := strings.Repeat("x", maxAdmissionBodyBytes+1)
	rec := post(handler, []byte(big), "application/json", "Bearer "+testToken)
	assertError(t, rec, http.StatusRequestEntityTooLarge, "contribution admission request exceeds the size cap")
	if admitter.calls() != 0 {
		t.Fatal("admitter invoked on oversized request")
	}

	// Chunked overflow: a lying ContentLength (-1) whose actual bytes
	// exceed the cap must still be capped, by the MaxBytesReader. The
	// body stays a valid JSON string token so the decoder keeps reading
	// until the cap trips rather than failing on syntax first.
	bigString := `{"goal":"` + strings.Repeat("x", maxAdmissionBodyBytes) + `"}`
	req := httptest.NewRequest(http.MethodPost, PathContributions, strings.NewReader(bigString))
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assertError(t, rec, http.StatusRequestEntityTooLarge, "contribution admission request exceeds the size cap")
	if admitter.calls() != 0 {
		t.Fatal("admitter invoked on chunked overflow")
	}
}

func TestDecodeRejectsMalformedShapes(t *testing.T) {
	admitter := &fakeAdmitter{}
	handler := newTestServer(t, admitter).Handler()

	cases := map[string]string{
		"malformed json":      `{"lane": `,
		"json array":          `[1, 2, 3]`,
		"unknown field":       `{"lane":"local","baseRepo":"u/p","baseRef":"main","workRepo":"c/p","goal":"g","sneaky":"field"}`,
		"trailing garbage":    `{"lane":"local","baseRepo":"u/p","baseRef":"main","workRepo":"c/p","goal":"g"} trailing`,
		"second object":       `{"lane":"local","baseRepo":"u/p","baseRef":"main","workRepo":"c/p","goal":"g"} {"goal":"again"}`,
		"empty body":          ``,
		"wrong value type":    `{"lane":42}`,
		"fresh and key":       `{"lane":"local","baseRepo":"u/p","baseRef":"main","workRepo":"c/p","goal":"g","fresh":true,"idempotencyKey":"k1"}`,
		"fresh and blank key": `{"lane":"local","baseRepo":"u/p","baseRef":"main","workRepo":"c/p","goal":"g","fresh":true,"idempotencyKey":"   "}`,
	}
	for name, body := range cases {
		rec := postRaw(t, handler, body)
		assertError(t, rec, http.StatusBadRequest, "")
		if admitter.calls() != 0 {
			t.Fatalf("%s: admitter invoked on rejected body", name)
		}
	}
}

func TestAdapterInvalidAdmissionMapsTo400(t *testing.T) {
	// A fake returning the adapter's own sentinel proves the mapping; the
	// integration test below proves the real adapter path too.
	admitter := &fakeAdmitter{err: fmt.Errorf("%w: goal is required", ErrInvalidAdmission)}
	handler := newTestServer(t, admitter).Handler()

	rec := postJSON(t, handler, validRequestBody())
	assertError(t, rec, http.StatusBadRequest, "adhoc: invalid admission: goal is required")
}

func TestOwnershipBoundaryRejectsBeforeClusterContact(t *testing.T) {
	admitter := &fakeAdmitter{}
	handler := newTestServer(t, admitter).Handler()

	body := validRequestBody()
	body["namespace"] = "elsewhere"
	rec := postJSON(t, handler, body)
	assertError(t, rec, http.StatusForbidden, "contribution admission targets a namespace outside the owned namespaces")
	if admitter.calls() != 0 {
		t.Fatal("admitter invoked for an out-of-bounds namespace")
	}

	// A member namespace proceeds.
	body["namespace"] = "contrib"
	rec = postJSON(t, handler, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("owned namespace: status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestNamespaceDefaultsAndReachesAdmitter(t *testing.T) {
	admitter := &fakeAdmitter{result: Admitted{RunName: "run-1", WorkItemID: "k"}}
	handler := newTestServer(t, admitter).Handler()

	rec := postJSON(t, handler, validRequestBody())
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if len(admitter.admissions) != 1 {
		t.Fatalf("admissions = %d, want 1", len(admitter.admissions))
	}
	got := admitter.admissions[0]
	if got.Namespace != "courier" {
		t.Fatalf("namespace = %q, want the default %q", got.Namespace, "courier")
	}
	if got.Lane != "local" || got.BaseRepo != "upstream/project" || got.BaseRef != "main" ||
		got.WorkRepo != "contributor/project" || got.Goal != "fix the login flow" {
		t.Fatalf("admission = %#v, want the request fields carried through", got)
	}
	if got.IdempotencyKey != "" {
		t.Fatalf("idempotency key = %q, want empty (adapter mints)", got.IdempotencyKey)
	}
	if got.Debug {
		t.Fatal("debug = true, want false")
	}

	// An explicit namespace is passed through.
	body := validRequestBody()
	body["namespace"] = "contrib"
	if rec := postJSON(t, handler, body); rec.Code != http.StatusCreated {
		t.Fatalf("explicit namespace: status = %d, want 201", rec.Code)
	}
	if got := admitter.admissions[1].Namespace; got != "contrib" {
		t.Fatalf("explicit namespace = %q, want contrib", got)
	}
}

func TestDebugAndUpstreamIssueCarried(t *testing.T) {
	admitter := &fakeAdmitter{result: Admitted{RunName: "run-1", WorkItemID: "k"}}
	handler := newTestServer(t, admitter).Handler()

	body := validRequestBody()
	body["upstreamIssue"] = "https://github.com/upstream/project/issues/7"
	body["debug"] = true
	rec := postJSON(t, handler, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	got := admitter.admissions[0]
	if got.UpstreamIssue != "https://github.com/upstream/project/issues/7" {
		t.Fatalf("upstreamIssue = %q", got.UpstreamIssue)
	}
	if !got.Debug {
		t.Fatal("debug = false, want true")
	}
}

func TestCreatedAndResumedResponses(t *testing.T) {
	admitter := &fakeAdmitter{result: Admitted{RunName: "courier-abc", WorkItemID: "k1", Resumed: false}}
	handler := newTestServer(t, admitter).Handler()

	rec := postJSON(t, handler, validRequestBody())
	assertNoTokenSweep(t, rec)
	if rec.Code != http.StatusCreated {
		t.Fatalf("created: status = %d, want 201", rec.Code)
	}
	var result AdmissionResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result != (AdmissionResult{Run: "courier-abc", Namespace: "courier", WorkItemID: "k1", Resumed: false}) {
		t.Fatalf("result = %#v", result)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type = %q, want application/json", ct)
	}

	admitter.result = Admitted{RunName: "courier-abc", WorkItemID: "k1", Resumed: true}
	rec = postJSON(t, handler, validRequestBody())
	if rec.Code != http.StatusOK {
		t.Fatalf("resumed: status = %d, want 200", rec.Code)
	}
	result = AdmissionResult{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if !result.Resumed {
		t.Fatalf("resumed result = %#v, want Resumed=true", result)
	}
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code int
		msg  string
	}{
		{"foreign run", fmt.Errorf("%w: coder run x is owned by a different admission", ErrForeignRun), http.StatusConflict, "adhoc: coder run belongs to a different admission: coder run x is owned by a different admission"},
		{"deadline", context.DeadlineExceeded, http.StatusServiceUnavailable, "contribution admission timed out before the API server answered"},
		{"wrapped deadline", fmt.Errorf("get coder run: %w", context.DeadlineExceeded), http.StatusServiceUnavailable, "contribution admission timed out before the API server answered"},
		{"generic", errors.New("get coder run: connection refused"), http.StatusInternalServerError, "contribution admission failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admitter := &fakeAdmitter{err: tc.err}
			handler := newTestServer(t, admitter).Handler()
			rec := postJSON(t, handler, validRequestBody())
			// The fixed 503/500 messages must not carry the underlying
			// error text, which may name cluster internals.
			assertError(t, rec, tc.code, tc.msg)
		})
	}
}

// TestAdmissionCarriesDeadline pins the deadline enforcement itself: the
// context the handler hands the admitter must carry a live deadline inside
// the admission bound, not merely map a fake DeadlineExceeded to a 503.
func TestAdmissionCarriesDeadline(t *testing.T) {
	admitter := &deadlineAdmitter{}
	handler := newTestServer(t, admitter).Handler()

	rec := postJSON(t, handler, validRequestBody())
	assertNoTokenSweep(t, rec)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if !admitter.saw {
		t.Fatal("admission context carried no live deadline within the admission bound")
	}
}

// TestConstructorFailsClosed pins that NewAdmissionServer rejects a wiring
// mistake at construction: a server without an admitter, a token, or a bind
// address would do nothing or authenticate nothing.
func TestConstructorFailsClosed(t *testing.T) {
	valid := AdmissionServerConfig{
		Admitter: &fakeAdmitter{},
		Token:    testToken,
		Bind:     ":0",
	}
	if _, err := NewAdmissionServer(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mutate := range map[string]func(*AdmissionServerConfig){
		"nil admitter": func(c *AdmissionServerConfig) { c.Admitter = nil },
		"empty token":  func(c *AdmissionServerConfig) { c.Token = "" },
		"empty bind":   func(c *AdmissionServerConfig) { c.Bind = "" },
	} {
		cfg := valid
		mutate(&cfg)
		if _, err := NewAdmissionServer(cfg); err == nil {
			t.Fatalf("%s: construction accepted a required missing input", name)
		}
	}
}

func TestFreshMintsUniqueKeys(t *testing.T) {
	admitter := &fakeAdmitter{result: Admitted{RunName: "run-1", WorkItemID: "k"}}
	handler := newTestServer(t, admitter).Handler()

	body := validRequestBody()
	body["fresh"] = true
	rec := postJSON(t, handler, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	rec = postJSON(t, handler, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("second fresh call: status = %d, want 201", rec.Code)
	}
	if len(admitter.admissions) != 2 {
		t.Fatalf("admissions = %d, want 2", len(admitter.admissions))
	}
	first, second := admitter.admissions[0].IdempotencyKey, admitter.admissions[1].IdempotencyKey
	if !mintedAPIKeyRe.MatchString(first) {
		t.Fatalf("minted key %q does not match the api key shape", first)
	}
	if !mintedAPIKeyRe.MatchString(second) {
		t.Fatalf("minted key %q does not match the api key shape", second)
	}
	if first == second {
		t.Fatalf("two fresh calls minted the same key %q", first)
	}
}

func TestExplicitKeyReachesAdmitter(t *testing.T) {
	admitter := &fakeAdmitter{result: Admitted{RunName: "run-1", WorkItemID: "k1", Resumed: true}}
	handler := newTestServer(t, admitter).Handler()

	body := validRequestBody()
	body["idempotencyKey"] = "caller-key-1"
	rec := postJSON(t, handler, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := admitter.admissions[0].IdempotencyKey; got != "caller-key-1" {
		t.Fatalf("idempotency key = %q, want caller-key-1", got)
	}
}

// TestAdapterIntegration proves the server is a thin frontend over the
// real adapter's durable-identity semantics: same key resumes the same
// run, a new key creates a second run, and the run name is the one
// source.RunName derives — exactly what courierctl contribute produces.
func TestAdapterIntegration(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add courier scheme: %v", err)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	srv, err := NewAdmissionServer(AdmissionServerConfig{
		Admitter:         New(kubeClient, kubeClient),
		Token:            testToken,
		Bind:             ":0",
		DefaultNamespace: "courier",
		OwnedNamespaces:  []string{"courier"},
	})
	if err != nil {
		t.Fatalf("construct admission server: %v", err)
	}
	handler := srv.Handler()

	const key = "integration-key-1"
	body := validRequestBody()
	body["namespace"] = "courier"
	body["idempotencyKey"] = key

	rec := postJSON(t, handler, body)
	assertNoTokenSweep(t, rec)
	if rec.Code != http.StatusCreated {
		t.Fatalf("first submission: status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	var result AdmissionResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	wantName := source.RunName(SourceName, key)
	if result != (AdmissionResult{Run: wantName, Namespace: "courier", WorkItemID: key, Resumed: false}) {
		t.Fatalf("first result = %#v, want run %s", result, wantName)
	}
	if got := countRuns(t, kubeClient, "courier"); got != 1 {
		t.Fatalf("runs after first submission = %d, want 1", got)
	}

	// Same key: resume, no second run.
	rec = postJSON(t, handler, body)
	assertNoTokenSweep(t, rec)
	if rec.Code != http.StatusOK {
		t.Fatalf("duplicate submission: status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	result = AdmissionResult{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result != (AdmissionResult{Run: wantName, Namespace: "courier", WorkItemID: key, Resumed: true}) {
		t.Fatalf("duplicate result = %#v, want resume of %s", result, wantName)
	}
	if got := countRuns(t, kubeClient, "courier"); got != 1 {
		t.Fatalf("runs after duplicate = %d, want 1", got)
	}

	// A different key is an intentional new contribution: a second run.
	other := validRequestBody()
	other["namespace"] = "courier"
	other["idempotencyKey"] = "integration-key-2"
	rec = postJSON(t, handler, other)
	if rec.Code != http.StatusCreated {
		t.Fatalf("new-key submission: status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := countRuns(t, kubeClient, "courier"); got != 2 {
		t.Fatalf("runs after new key = %d, want 2", got)
	}
}

// countRuns lists the CoderRuns in a namespace.
func countRuns(t *testing.T, c client.Client, namespace string) int {
	t.Helper()
	list := &courierv1alpha1.CoderRunList{}
	if err := c.List(context.Background(), list, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list coder runs: %v", err)
	}
	return len(list.Items)
}
