package adhoc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testReadToken is the read-path bearer token, deliberately distinct from
// testToken so every auth test proves the two credentials are not
// interchangeable.
const testReadToken = "test-read-token-not-a-real-secret"

// fakeStatusReader records the namespace and run of every call and returns
// a configurable result; a non-nil block channel makes Status wait until
// the channel is closed, so the concurrency bound is exercised against
// genuinely in-flight reads.
type fakeStatusReader struct {
	status ContributionStatus
	err    error
	block  chan struct{}

	mu      sync.Mutex
	calls   int
	lastNS  string
	lastRun string
}

func (f *fakeStatusReader) Status(ctx context.Context, namespace, run string) (ContributionStatus, error) {
	f.mu.Lock()
	f.calls++
	f.lastNS, f.lastRun = namespace, run
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ContributionStatus{}, ctx.Err()
		}
	}
	return f.status, f.err
}

func (f *fakeStatusReader) called() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeStatusReader) args() (string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastNS, f.lastRun
}

// statusServer builds a server with reads wired to the reader, sharing the
// write-path wiring of newTestServer so both surfaces ride one config.
func statusServer(t *testing.T, reader StatusReader) *AdmissionServer {
	t.Helper()
	srv, err := NewAdmissionServer(AdmissionServerConfig{
		Admitter:         &fakeAdmitter{},
		Token:            testToken,
		Bind:             ":0",
		DefaultNamespace: "courier",
		OwnedNamespaces:  []string{"courier", "contrib"},
		Status:           reader,
		ReadToken:        testReadToken,
	})
	if err != nil {
		t.Fatalf("construct admission server with reads: %v", err)
	}
	return srv
}

// get issues one GET against the handler; tests control the credential
// exactly as the post helper does for writes.
func get(handler http.Handler, target, authorization string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// assertNoSecrets sweeps the response for both credentials; every status
// response in every case goes through it.
func assertNoSecrets(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	for _, secret := range []string{testToken, testReadToken} {
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("response body contains a bearer token: %q", rec.Body.String())
		}
		for name, values := range rec.Header() {
			for _, value := range values {
				if strings.Contains(value, secret) {
					t.Fatalf("response header %q contains a bearer token: %q", name, value)
				}
			}
		}
	}
}

func TestStatusReadsDisabledWithoutReader(t *testing.T) {
	// newTestServer wires no reader and no read token: the read surface
	// must not exist at all. Every method on a run path is a route miss
	// with the admission-only miss message — no 405 method map and no
	// message naming a route the server will never serve.
	handler := newTestServer(t, &fakeAdmitter{}).Handler()
	for _, auth := range []string{"", "Bearer " + testToken, "Bearer " + testReadToken} {
		for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
			req := httptest.NewRequest(method, "/v1/contributions/courier-abc", nil)
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assertNoSecrets(t, rec)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s: status = %d, want 404 (body: %s)", method, rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "status") {
				t.Fatalf("%s: miss response announces the status surface: %s", method, rec.Body.String())
			}
		}
	}
}

// TestAdmissionRejectsReadToken pins the other half of credential
// separation: on a server wired with both tokens, the read bearer is not
// an admission credential.
func TestAdmissionRejectsReadToken(t *testing.T) {
	admitter := &fakeAdmitter{result: Admitted{RunName: "run-1", WorkItemID: "k"}}
	srv, err := NewAdmissionServer(AdmissionServerConfig{
		Admitter:         admitter,
		Token:            testToken,
		Bind:             ":0",
		DefaultNamespace: "courier",
		Status:           &fakeStatusReader{},
		ReadToken:        testReadToken,
	})
	if err != nil {
		t.Fatalf("construct admission server: %v", err)
	}
	handler := srv.Handler()

	body, err := json.Marshal(validRequestBody())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rec := post(handler, body, "application/json", "Bearer "+testReadToken)
	assertError(t, rec, http.StatusUnauthorized, "contribution admission requires a valid bearer token")
	if admitter.calls() != 0 {
		t.Fatal("read token reached the admitter")
	}

	// The admission token still admits, unchanged by the read wiring.
	if rec := postJSON(t, handler, validRequestBody()); rec.Code != http.StatusCreated {
		t.Fatalf("admission with its own token: status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestStatusAuthRejectsEveryOtherCredential(t *testing.T) {
	reader := &fakeStatusReader{}
	handler := statusServer(t, reader).Handler()

	for name, auth := range map[string]string{
		"missing":           "",
		"non-bearer":        "Basic " + testReadToken,
		"wrong token":       "Bearer wrong-token",
		"empty bearer":      "Bearer ",
		"admission token":   "Bearer " + testToken,
		"read token padded": "Bearer  " + testReadToken[1:],
	} {
		rec := get(handler, "/v1/contributions/courier-abc", auth)
		assertError(t, rec, http.StatusUnauthorized, "contribution status reads require a valid bearer token")
		if reader.called() != 0 {
			t.Fatalf("%s: reader invoked on rejected request", name)
		}
	}
}

func TestStatusMethodAndRouteDiscipline(t *testing.T) {
	reader := &fakeStatusReader{}
	handler := statusServer(t, reader).Handler()

	// A run path accepts GET only; POST/PUT/DELETE name the read mistake.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/v1/contributions/courier-abc", nil)
		req.Header.Set("Authorization", "Bearer "+testReadToken)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assertError(t, rec, http.StatusMethodNotAllowed, "contribution status reads accept GET only")
	}

	// Deeper paths and the empty segment are route misses, even with a
	// valid credential: the read never becomes a nested lookup.
	for _, path := range []string{"/v1/contributions/", "/v1/contributions/a/b", "/v1/other", "/"} {
		rec := get(handler, path, "Bearer "+testReadToken)
		assertNoSecrets(t, rec)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("GET %s: status = %d, want 404 (body: %s)", path, rec.Code, rec.Body.String())
		}
	}
	if reader.called() != 0 {
		t.Fatal("reader invoked on wrong method or miss path")
	}
}

func TestStatusRunShapeRejectedBeforeReader(t *testing.T) {
	reader := &fakeStatusReader{}
	handler := statusServer(t, reader).Handler()

	oversized := strings.Repeat("a", 254)
	for _, run := range []string{"Bad_Name", "run!", ".hidden", "-lead", "trail-", oversized, "%2E%2E"} {
		rec := get(handler, "/v1/contributions/"+run, "Bearer "+testReadToken)
		assertNoSecrets(t, rec)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("run %q: status = %d, want 404 (body: %s)", run, rec.Code, rec.Body.String())
		}
	}
	if reader.called() != 0 {
		t.Fatal("malformed run names reached the reader")
	}
}

func TestStatusNamespaceDefaultingAndBoundary(t *testing.T) {
	reader := &fakeStatusReader{}
	handler := statusServer(t, reader).Handler()

	// No parameter: the default namespace reaches the reader.
	if rec := get(handler, "/v1/contributions/courier-abc", "Bearer "+testReadToken); rec.Code != http.StatusOK {
		t.Fatalf("defaulted read: status = %d (body: %s)", rec.Code, rec.Body.String())
	}
	if ns, run := reader.args(); ns != "courier" || run != "courier-abc" {
		t.Fatalf("reader saw %q/%q, want courier/courier-abc", ns, run)
	}

	// A member namespace is honored.
	if rec := get(handler, "/v1/contributions/courier-abc?namespace=contrib", "Bearer "+testReadToken); rec.Code != http.StatusOK {
		t.Fatalf("owned namespace: status = %d (body: %s)", rec.Code, rec.Body.String())
	}
	if ns, _ := reader.args(); ns != "contrib" {
		t.Fatalf("reader saw namespace %q, want contrib", ns)
	}

	// An out-of-bounds namespace is refused before any lookup.
	before := reader.called()
	rec := get(handler, "/v1/contributions/courier-abc?namespace=elsewhere", "Bearer "+testReadToken)
	assertError(t, rec, http.StatusForbidden, "contribution status reads target a namespace outside the owned namespaces")
	if reader.called() != before {
		t.Fatal("reader invoked for an out-of-bounds namespace")
	}
}

func TestStatusErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		code       int
		msg        string
		retryAfter string
	}{
		{"not found", ErrContributionNotFound, http.StatusNotFound, "contribution run not found", ""},
		{"wrapped not found", fmt.Errorf("lookup: %w", ErrContributionNotFound), http.StatusNotFound, "contribution run not found", ""},
		{"deadline", context.DeadlineExceeded, http.StatusServiceUnavailable, "contribution status read timed out before the cluster answered", "1"},
		{"wrapped deadline", fmt.Errorf("get coder run: %w", context.DeadlineExceeded), http.StatusServiceUnavailable, "contribution status read timed out before the cluster answered", "1"},
		{"generic", errors.New("get coder run: connection refused from etcd"), http.StatusInternalServerError, "contribution status read failed", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeStatusReader{err: tc.err}
			handler := statusServer(t, reader).Handler()
			rec := get(handler, "/v1/contributions/courier-abc", "Bearer "+testReadToken)
			// The fixed messages carry no underlying error text.
			assertError(t, rec, tc.code, tc.msg)
			if strings.Contains(rec.Body.String(), "etcd") {
				t.Fatal("cluster error text leaked into the response")
			}
			// Every read-surface 503 names when to come back; the other
			// statuses carry no such hint.
			if got := rec.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
		})
	}
}

func TestStatusHappyResponse(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC))
	reader := &fakeStatusReader{status: ContributionStatus{
		Run:         "courier-abc",
		Namespace:   "courier",
		Lane:        "local",
		BaseRepo:    "upstream/project",
		WorkRepo:    "contributor/project",
		Phase:       "Verifying",
		CIParked:    true,
		Publication: &ContributionPublication{Branch: "b", HeadRepo: "contributor/project", HeadSHA: "sha", PR: "42"},
		AdmittedAt:  &now,
		ObservedAt:  now,
	}}
	handler := statusServer(t, reader).Handler()

	rec := get(handler, "/v1/contributions/courier-abc", "Bearer "+testReadToken)
	assertNoSecrets(t, rec)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type = %q, want application/json", ct)
	}
	var got ContributionStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if got.Phase != "Verifying" || !got.CIParked || got.Publication == nil || got.Publication.PR != "42" {
		t.Fatalf("status = %#v, want the reader's projection carried through", got)
	}
	if got.ObservedAt.IsZero() {
		t.Fatal("response carries no observation timestamp")
	}
	if strings.Contains(rec.Body.String(), "workItemId") {
		t.Fatalf("response carries the untrusted work item id: %s", rec.Body.String())
	}
}

// TestStatusFixedShapeResponse pins the fixed-shape claim: the scalar
// fields are always present, at zero values when the run carries none,
// so consumers never branch on key presence.
func TestStatusFixedShapeResponse(t *testing.T) {
	reader := &fakeStatusReader{status: ContributionStatus{
		Run:       "courier-abc",
		Namespace: "courier",
		Lane:      "local",
		BaseRepo:  "upstream/project",
		Phase:     "Pending",
	}}
	handler := statusServer(t, reader).Handler()
	rec := get(handler, "/v1/contributions/courier-abc", "Bearer "+testReadToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &fields); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	for _, key := range []string{"run", "namespace", "lane", "baseRepo", "workRepo", "phase", "ciParked", "observedAt"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("fixed-shape field %q absent from %s", key, rec.Body.String())
		}
	}
}

func TestStatusConcurrencyBound(t *testing.T) {
	release := make(chan struct{})
	reader := &fakeStatusReader{block: release}
	handler := statusServer(t, reader).Handler()

	inFlight := make(chan *httptest.ResponseRecorder, maxConcurrentStatusReads)
	var wg sync.WaitGroup
	for range maxConcurrentStatusReads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inFlight <- get(handler, "/v1/contributions/courier-abc", "Bearer "+testReadToken)
		}()
	}
	// Saturate: the bound is exercised once every slot is genuinely
	// occupied, not assumed.
	for reader.called() < maxConcurrentStatusReads {
		time.Sleep(5 * time.Millisecond)
	}

	rec := get(handler, "/v1/contributions/courier-abc", "Bearer "+testReadToken)
	assertNoSecrets(t, rec)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated read: status = %d, want 503 (body: %s)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", rec.Header().Get("Retry-After"))
	}
	if reader.called() != maxConcurrentStatusReads {
		t.Fatalf("reader served %d reads with %d in flight", reader.called(), maxConcurrentStatusReads)
	}

	close(release)
	wg.Wait()
	close(inFlight)
	for blocked := range inFlight {
		if blocked.Code != http.StatusOK {
			t.Fatalf("blocked read: status = %d, want 200", blocked.Code)
		}
	}

	// Slots return: reads are served again after release.
	if rec := get(handler, "/v1/contributions/courier-abc", "Bearer "+testReadToken); rec.Code != http.StatusOK {
		t.Fatalf("post-release read: status = %d, want 200", rec.Code)
	}
}

// TestStatusAdapterProjection proves the real read path over fixture
// CoderRun states: the controller's recorded status is the only source,
// the projection distinguishes the documented lifecycle states, a foreign
// run and a miss are both 404, and the response carries no forbidden
// content.
func TestStatusAdapterProjection(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add courier scheme: %v", err)
	}
	now := metav1.NewTime(time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC))

	contribution := func(name string, mutate func(*courierv1alpha1.CoderRun)) *courierv1alpha1.CoderRun {
		run := &courierv1alpha1.CoderRun{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "courier"},
			Spec: courierv1alpha1.CoderRunSpec{
				Mode:       courierv1alpha1.ModeContribute,
				Source:     SourceName,
				WorkItemID: "k-" + name,
				Repo:       "upstream/project",
				WorkRepo:   "contributor/project",
				Lane:       "local",
			},
		}
		mutate(run)
		return run
	}

	objects := []runtime.Object{
		// Admitted, controller has not stamped a phase yet.
		contribution("run-admitted", func(*courierv1alpha1.CoderRun) {}),
		// Running with a recorded branch and head.
		contribution("run-running", func(r *courierv1alpha1.CoderRun) {
			r.Status.Phase = courierv1alpha1.PhaseRunning
			r.Status.Branch = "courier/upstream/project/goal-abc"
			r.Status.HeadSHA = "0123456789abcdef"
			r.Status.AdmittedAt = &now
			r.Status.StartedAt = &now
		}),
		// Verifying, CI-parked, PR recorded.
		contribution("run-parked", func(r *courierv1alpha1.CoderRun) {
			r.Status.Phase = courierv1alpha1.PhaseVerifying
			r.Status.CIPark = &courierv1alpha1.CIParkState{}
			r.Status.PR = "https://github.com/upstream/project/pull/42"
		}),
		// HandedOff straight out of a CI park: finishCIPark ends the run
		// while retaining the park record as the attempt's durable
		// account, so the read must report the run as no longer parked.
		contribution("run-handed-off", func(r *courierv1alpha1.CoderRun) {
			r.Status.Phase = courierv1alpha1.PhaseHandedOff
			r.Status.CIPark = &courierv1alpha1.CIParkState{
				Head:        "0123456789abcdef",
				LastOutcome: courierv1alpha1.CIParkOutcomeExhausted,
			}
			r.Status.PR = "https://github.com/upstream/project/pull/42"
			r.Status.FinishedAt = &now
		}),
		// Failed with a recorded terminal diagnostic.
		contribution("run-failed", func(r *courierv1alpha1.CoderRun) {
			r.Status.Phase = courierv1alpha1.PhaseFailed
			r.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{
				Category: courierv1alpha1.TerminalDiagnosticCategoryExecutorCrash,
				Reason:   "provider returned a refusal",
				Outcome:  "blocked_external",
			}
			r.Status.FinishedAt = &now
		}),
		// A dispatch run with the shape of a contribution: foreign.
		contribution("run-foreign", func(r *courierv1alpha1.CoderRun) {
			r.Spec.Source = "dispatch"
			r.Status.Phase = courierv1alpha1.PhaseRunning
		}),
		// A run carrying fields beyond the projection cap (admission
		// never produces these; the cap is the defense-in-depth answer
		// to CRD fields that are not themselves length-bounded).
		contribution("run-oversized", func(r *courierv1alpha1.CoderRun) {
			r.Spec.WorkItemID = strings.Repeat("w", maxStatusFieldBytes+1)
			r.Status.Phase = courierv1alpha1.PhaseVerifying
			r.Status.Branch = "courier/upstream/project/goal-abc"
			r.Status.PR = strings.Repeat("x", maxStatusFieldBytes+1)
		}),
	}

	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	srv, err := NewAdmissionServer(AdmissionServerConfig{
		Admitter:         New(kubeClient, kubeClient),
		Token:            testToken,
		Bind:             ":0",
		DefaultNamespace: "courier",
		OwnedNamespaces:  []string{"courier"},
		Status:           New(kubeClient, kubeClient),
		ReadToken:        testReadToken,
	})
	if err != nil {
		t.Fatalf("construct admission server: %v", err)
	}
	handler := srv.Handler()

	fetch := func(name string) ContributionStatus {
		t.Helper()
		rec := get(handler, "/v1/contributions/"+name, "Bearer "+testReadToken)
		assertNoSecrets(t, rec)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: status = %d (body: %s)", name, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "provider returned a refusal") {
			t.Fatalf("GET %s: diagnostic prose leaked into the response", name)
		}
		// The idempotency key is untrusted caller text that admission
		// already returned: no read response ever re-serves it.
		if strings.Contains(rec.Body.String(), "workItemId") {
			t.Fatalf("GET %s: untrusted work item id re-served: %s", name, rec.Body.String())
		}
		var status ContributionStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		if status.ObservedAt.IsZero() {
			t.Fatalf("GET %s: no observation timestamp", name)
		}
		return status
	}

	admitted := fetch("run-admitted")
	if admitted.Phase != PhaseUnknown || admitted.CIParked || admitted.Publication != nil || admitted.Cause != nil {
		t.Fatalf("admitted projection = %#v, want unknown phase and nothing published", admitted)
	}
	if admitted.BaseRepo != "upstream/project" || admitted.WorkRepo != "contributor/project" || admitted.Lane != "local" {
		t.Fatalf("admitted projection = %#v, want the admitted identity echoed", admitted)
	}
	if admitted.AdmittedAt != nil {
		t.Fatal("admitted projection carries a timestamp the fixture never set")
	}

	running := fetch("run-running")
	if running.Phase != "Running" || running.Publication == nil ||
		running.Publication.Branch != "courier/upstream/project/goal-abc" ||
		running.Publication.HeadSHA != "0123456789abcdef" ||
		running.AdmittedAt == nil || running.StartedAt == nil {
		t.Fatalf("running projection = %#v", running)
	}

	parked := fetch("run-parked")
	if parked.Phase != "Verifying" || !parked.CIParked || parked.Publication == nil || parked.Publication.PR == "" {
		t.Fatalf("parked projection = %#v", parked)
	}

	// The retained park account of a finished run is history, not a
	// live park: ciParked stays false at a recorded ending.
	handedOff := fetch("run-handed-off")
	if handedOff.Phase != "HandedOff" || handedOff.CIParked || handedOff.FinishedAt == nil {
		t.Fatalf("handed-off projection = %#v, want no live park", handedOff)
	}

	failed := fetch("run-failed")
	if failed.Phase != "Failed" || failed.FinishedAt == nil ||
		failed.Cause == nil || failed.Cause.Category != "executor-crash" || failed.Cause.Outcome != "blocked_external" {
		t.Fatalf("failed projection = %#v", failed)
	}

	// Oversized values are dropped whole: no truncated identity is ever
	// presented as a complete one, and the rest of the projection rides
	// on unaffected. The oversized idempotency key in the fixture is
	// never re-served at all (pinned in fetch).
	oversized := fetch("run-oversized")
	if oversized.Publication == nil ||
		oversized.Publication.Branch != "courier/upstream/project/goal-abc" ||
		oversized.Publication.PR != "" {
		t.Fatalf("oversized projection = %#v", oversized)
	}

	// The foreign run and a miss are indistinguishable 404s with the
	// same message: the endpoint never confirms foreign objects exist.
	for _, name := range []string{"run-foreign", "run-missing"} {
		rec := get(handler, "/v1/contributions/"+name, "Bearer "+testReadToken)
		assertError(t, rec, http.StatusNotFound, "contribution run not found")
	}
}

// TestStatusReadsViaAPIReaderNotCacheClient pins the read consistency of
// the status path: it resolves the run through the direct API-server
// reader, like the admission path's create-vs-resume lookups, so a
// lagging informer cache can never serve a poller a state the controller
// has already moved past.
func TestStatusReadsViaAPIReaderNotCacheClient(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add courier scheme: %v", err)
	}
	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run-live", Namespace: "courier"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeContribute,
			Source:     SourceName,
			WorkItemID: "k-live",
			Repo:       "upstream/project",
			WorkRepo:   "contributor/project",
			Lane:       "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: courierv1alpha1.PhaseRunning},
	}
	// The cache client knows nothing; the API reader has the run. A
	// cache-backed read would answer NotFound (and the handler a 404).
	laggingCache := fake.NewClientBuilder().WithScheme(scheme).Build()
	apiReader := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(run).Build()

	adapter := New(laggingCache, apiReader)
	got, err := adapter.Status(context.Background(), "courier", "run-live")
	if err != nil {
		t.Fatalf("status via api reader: %v", err)
	}
	if got.Run != "run-live" || got.Phase != "Running" {
		t.Fatalf("projection = %#v, want the api reader's run", got)
	}

	// The nil-reader fallback still serves through the client, and a nil
	// client still fails closed at the call site.
	if _, err := New(apiReader, nil).Status(context.Background(), "courier", "run-live"); err != nil {
		t.Fatalf("nil-reader fallback: %v", err)
	}
	if _, err := (Adapter{}).Status(context.Background(), "courier", "run-live"); !errors.Is(err, ErrClientRequired) {
		t.Fatalf("nil client: err = %v, want ErrClientRequired", err)
	}
}

// TestBoundedFieldDropsOversized pins the cap boundary: a value at the cap
// survives, one byte past it is dropped whole, never truncated.
func TestBoundedFieldDropsOversized(t *testing.T) {
	atCap := strings.Repeat("a", maxStatusFieldBytes)
	if got := boundedField(atCap); got != atCap {
		t.Fatal("a value at the cap must survive")
	}
	if got := boundedField(atCap + "x"); got != "" {
		t.Fatalf("a value past the cap must be dropped, got %q", got[:min(len(got), 32)])
	}
}

// TestConstructorRejectsHalfWiredReads pins the paired construction of
// the read surface.
func TestConstructorRejectsHalfWiredReads(t *testing.T) {
	base := AdmissionServerConfig{
		Admitter: &fakeAdmitter{},
		Token:    testToken,
		Bind:     ":0",
	}
	halfA := base
	halfA.ReadToken = testReadToken
	if _, err := NewAdmissionServer(halfA); err == nil {
		t.Fatal("construction accepted a read token without a status reader")
	}
	halfB := base
	halfB.Status = &fakeStatusReader{}
	if _, err := NewAdmissionServer(halfB); err == nil {
		t.Fatal("construction accepted a status reader without a read token")
	}
	both := base
	both.ReadToken = testReadToken
	both.Status = &fakeStatusReader{}
	if _, err := NewAdmissionServer(both); err != nil {
		t.Fatalf("paired read wiring rejected: %v", err)
	}
}
