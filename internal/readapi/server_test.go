package readapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

const testToken = "test-read-token"

// serverFixture builds a disabled-bind server over a fake client holding
// objs, and returns the server and its authenticated handler for
// httptest-driven requests without a listener. The server carries no
// KnownSources provider at all.
func serverFixture(t *testing.T, objs ...ctrlclient.Object) (*Server, http.Handler) {
	t.Helper()
	return serverFixtureWithProvider(t, nil, objs...)
}

// serverFixtureWithSources is serverFixture with a configured source-name
// inventory wired through ServerConfig.KnownSources.
func serverFixtureWithSources(t *testing.T, knownSources []string, objs ...ctrlclient.Object) (*Server, http.Handler) {
	t.Helper()
	return serverFixtureWithProvider(t, func() []string { return knownSources }, objs...)
}

func serverFixtureWithProvider(t *testing.T, knownSources func() []string, objs ...ctrlclient.Object) (*Server, http.Handler) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	server, err := NewServer(ServerConfig{
		Reader:       reader,
		Token:        testToken,
		Bind:         ":0",
		Namespace:    "courier",
		KnownSources: knownSources,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return server, server.Handler()
}

// get issues an authenticated GET against the handler and returns the
// recorder.
func get(handler http.Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func envelopeOf(t *testing.T, rec *httptest.ResponseRecorder) (Envelope[json.RawMessage], string) {
	t.Helper()
	var envelope Envelope[json.RawMessage]
	body := rec.Body.String()
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("response body is not a valid envelope: %v\n%s", err, body)
	}
	return envelope, body
}

// TestServerUnauthorized pins 401 on every route for a missing, malformed,
// and wrong token alike.
func TestServerUnauthorized(t *testing.T) {
	run := runFixture("run", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	_, handler := serverFixture(t, &run)

	paths := []string{PathOverview, PathLanes, PathSources, PathRuns, "/v1/runs/run"}
	for _, path := range paths {
		for name, token := range map[string]string{
			"missing":   "",
			"malformed": "Basic " + testToken,
			"empty":     "Bearer ",
			"wrong":     "Bearer not-the-token",
		} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			if token != "" {
				req.Header.Set("Authorization", token)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s with %s token on %s = %d, want 401", path, name, token, rec.Code)
			}
			if strings.Contains(rec.Body.String(), testToken) {
				t.Errorf("%s with %s token echoed token material: %s", path, name, rec.Body.String())
			}
		}
	}
}

// TestServerHealthzUnauthenticated pins the console-facing liveness route:
// GET /healthz answers 200 without credentials and carries no data — no
// envelope, no namespace, no counts — while every projection route behind
// the same handler still demands the token.
func TestServerHealthzUnauthenticated(t *testing.T) {
	run := runFixture("run", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	_, handler := serverFixture(t, &run)

	for name, token := range map[string]string{
		"missing": "",
		"wrong":   "Bearer not-the-token",
		"valid":   "Bearer " + testToken,
	} {
		req := httptest.NewRequest(http.MethodGet, PathHealthz, nil)
		if token != "" {
			req.Header.Set("Authorization", token)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("/healthz with %s token = %d, want 200", name, rec.Code)
		}
		body := rec.Body.String()
		if strings.Contains(body, "courier.misospace.dev") || strings.Contains(body, testToken) || strings.Contains(body, "courier") {
			t.Errorf("/healthz leaked state: %s", body)
		}
	}
}

// TestServerDisabled pins the bind-empty path: the constructor returns a
// no-op that never listens.
func TestServerDisabled(t *testing.T) {
	server, err := NewServer(ServerConfig{Bind: ""})
	if err != nil {
		t.Fatalf("NewServer with empty bind: %v", err)
	}
	if server == nil || !server.disabled {
		t.Fatalf("NewServer with empty bind = %+v, want a disabled no-op", server)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("disabled Start = %v, want nil", err)
		}
	case <-ctx.Done():
		t.Fatal("disabled Start did not return immediately")
	}
}

// TestServerConstructorFailsClosed pins the enabled-path construction
// errors.
func TestServerConstructorFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		config ServerConfig
	}{
		{
			name:   "missing token",
			config: ServerConfig{Bind: ":8084", Namespace: "courier"},
		},
		{
			name:   "missing namespace",
			config: ServerConfig{Bind: ":8084", Token: testToken},
		},
		{
			name:   "missing reader",
			config: ServerConfig{Bind: ":8084", Token: testToken, Namespace: "courier"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, err := NewServer(tc.config)
			if err == nil {
				t.Fatalf("NewServer = %+v, want an error", server)
			}
		})
	}
}

// TestServerHostileQueryParams pins the query-string rejection paths: every
// input that is not a well-formed bounded request is a 400, an unknown run
// is a 404, and a wrong method is a 405 — none of which panic.
func TestServerHostileQueryParams(t *testing.T) {
	run := runFixture("run", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	_, handler := serverFixture(t, &run)

	badQueries := []string{
		"?pageSize=99999999",
		"?pageSize=201",
		"?pageSize=0",
		"?pageSize=-1",
		"?pageSize=huge",
		fmt.Sprintf("?pageSize=%d", 1<<62),
		"?page=-1",
		"?page=minus",
		"?page=1e9x",
		// A page whose start offset would overflow int must be rejected,
		// not clamped or wrapped.
		fmt.Sprintf("?page=%d", int64(1)<<62),
		"?phase=Bogus",
		"?phase=" + url.QueryEscape(";DROP TABLE"),
	}
	for _, query := range badQueries {
		rec := get(handler, PathRuns+query)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s%s = %d, want 400 (%s)", PathRuns, query, rec.Code, rec.Body.String())
		}
	}

	if rec := get(handler, "/v1/runs/no-such-run"); rec.Code != http.StatusNotFound {
		t.Errorf("GET unknown run = %d, want 404", rec.Code)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, PathRuns, nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /v1/runs = %d, want 405", method, rec.Code)
		}
	}
}

// TestServerBoundedPagination pins the run-list window: default page size,
// the More indicator, offset pages, and the hard page-size cap.
func TestServerBoundedPagination(t *testing.T) {
	var runs []*courierv1alpha1.CoderRun
	base := testNow.Add(-time.Hour)
	for i := 0; i < 5; i++ {
		run := runFixture(fmt.Sprintf("run-%d", i), "github-label", "local", courierv1alpha1.PhaseRunning, base.Add(time.Duration(i)*time.Minute))
		runs = append(runs, &run)
	}
	_, handler := serverFixture(t, toObjects(runs)...)

	envelope, body := envelopeOf(t, get(handler, PathRuns))
	if envelope.Kind != KindRunList {
		t.Errorf("kind = %q, want %q", envelope.Kind, KindRunList)
	}
	var list RunSummaryList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v\n%s", err, body)
	}
	if len(list.Items) != 5 {
		t.Errorf("default page returned %d items, want 5", len(list.Items))
	}
	// Newest first.
	if list.Items[0].Name != "run-4" {
		t.Errorf("first item = %q, want run-4", list.Items[0].Name)
	}
	if list.Pagination.More {
		t.Errorf("More = true for a complete page")
	}
	if list.Pagination.Page != 0 {
		t.Errorf("Page = %d, want 0", list.Pagination.Page)
	}

	envelope, _ = envelopeOf(t, get(handler, PathRuns+"?pageSize=2&page=1"))
	var page1 RunSummaryList
	if err := json.Unmarshal(envelope.Payload, &page1); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if len(page1.Items) != 2 || page1.Items[0].Name != "run-2" {
		t.Errorf("page 1 = %+v, want run-2 and run-1", page1.Items)
	}
	if !page1.Pagination.More {
		t.Error("More = false with pages remaining")
	}

	envelope, _ = envelopeOf(t, get(handler, PathRuns+"?pageSize=2&page=2"))
	var page2 RunSummaryList
	if err := json.Unmarshal(envelope.Payload, &page2); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if len(page2.Items) != 1 || page2.Items[0].Name != "run-0" {
		t.Errorf("page 2 = %+v, want run-0 only", page2.Items)
	}
	if page2.Pagination.More {
		t.Error("More = true on the last page")
	}

	// The cap is enforced: above it the request is rejected, at it accepted.
	if rec := get(handler, PathRuns+"?pageSize=201"); rec.Code != http.StatusBadRequest {
		t.Errorf("pageSize above the cap = %d, want 400", rec.Code)
	}
	if rec := get(handler, PathRuns+"?pageSize=200"); rec.Code != http.StatusOK {
		t.Errorf("pageSize at the cap = %d, want 200", rec.Code)
	}
}

// TestServerFilters pins exact-match filtering and the phase-enum rejection.
func TestServerFilters(t *testing.T) {
	running := runFixture("run-running", "github-label", "local", courierv1alpha1.PhaseRunning, testNow.Add(-time.Minute))
	done := runFixture("run-done", "dispatch", "fast", courierv1alpha1.PhaseDone, testNow.Add(-2*time.Minute))
	_, handler := serverFixture(t, &running, &done)

	check := func(query string, want []string) {
		t.Helper()
		envelope, _ := envelopeOf(t, get(handler, PathRuns+query))
		var list RunSummaryList
		if err := json.Unmarshal(envelope.Payload, &list); err != nil {
			t.Fatalf("payload: %v", err)
		}
		var names []string
		for _, item := range list.Items {
			names = append(names, item.Name)
		}
		if fmt.Sprint(names) != fmt.Sprint(want) {
			t.Errorf("%s = %v, want %v", query, names, want)
		}
	}
	check("?lane=local", []string{"run-running"})
	check("?source=dispatch", []string{"run-done"})
	check("?phase=Done", []string{"run-done"})
	check("?lane=local&phase=Running", []string{"run-running"})
	check("?lane=no-such-lane", []string{})
	check("", []string{"run-running", "run-done"})
}

// TestServerHostileTextRoundTrip pushes control characters and injection
// text through the whole stack and requires clean JSON out.
func TestServerHostileTextRoundTrip(t *testing.T) {
	run := runFixture("run", "github-label", "local", courierv1alpha1.PhaseFailed, testNow.Add(-time.Minute))
	run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{
		Category: courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure,
		Reason:   "reason\x00\x1f\u202e<script>alert('xss')</script>\n\u000bInjected\u007f",
	}
	run.Status.Branch = "branch\x1f[injection]\x07"
	_, handler := serverFixture(t, &run)

	envelope, body := envelopeOf(t, get(handler, "/v1/runs/run"))
	if envelope.Kind != KindRunDetail {
		t.Errorf("kind = %q, want %q", envelope.Kind, KindRunDetail)
	}
	for i := 0; i < len(body); i++ {
		if body[i] < 0x20 && body[i] != '\n' {
			t.Errorf("raw control byte 0x%02x at offset %d in response body", body[i], i)
		}
	}
	if strings.Contains(body, "\x00") || strings.Contains(body, "\x1f") || strings.Contains(body, "\x07") {
		t.Error("control characters survived projection into the response")
	}
}

// TestServerRunDetailNotFoundAndEnvelope pins 404 for an unknown run and
// the envelope shape of the detail response.
func TestServerRunDetailNotFoundAndEnvelope(t *testing.T) {
	run := runFixture("run", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	_, handler := serverFixture(t, &run)

	if rec := get(handler, "/v1/runs/absent"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown run = %d, want 404", rec.Code)
	}
	envelope, _ := envelopeOf(t, get(handler, "/v1/runs/run"))
	if envelope.APIVersion != APIVersion || envelope.Kind != KindRunDetail {
		t.Errorf("envelope = %s/%s, want %s/%s", envelope.APIVersion, envelope.Kind, APIVersion, KindRunDetail)
	}
}

// TestServerRedactionEndToEnd pushes a fully populated run through the HTTP
// surface and requires every sensitive substring to be absent from the
// response body.
func TestServerRedactionEndToEnd(t *testing.T) {
	run := runFixture("run", "github-label", "local", courierv1alpha1.PhaseFailed, testNow)
	run.Spec.SourceContext = `SECRET-SOURCE-CONTEXT {"issue":"body"}`
	run.Status.SourceReceipt = "SECRET-SOURCE-RECEIPT-TOKEN"
	run.Status.Checkpoint = &courierv1alpha1.Checkpoint{
		Plan:            "SECRET-PLAN-TEXT",
		CompletedBriefs: []courierv1alpha1.Brief{{ID: "b1", Summary: "SECRET-BRIEF-SUMMARY", Commit: "abc123"}},
	}
	run.Status.LifecycleReportRetryToken = "SECRET-LAST-RETRY-TOKEN"
	run.Status.LifecycleReportActiveRetryToken = "SECRET-ACTIVE-RETRY-TOKEN"
	run.Status.CheckFingerprint = "SECRET-CHECK-FINGERPRINT"
	run.Status.FailedCheckFingerprint = "SECRET-FAILED-CHECK-FINGERPRINT"
	run.Status.Conditions = []metav1.Condition{
		{Type: courierv1alpha1.ConditionLifecycleReported, Status: metav1.ConditionTrue, Reason: "SECRET-LIFECYCLE-REASON ghp_AAAAABBBBBCCCCCDDDDDEEEEE", LastTransitionTime: metav1.NewTime(testNow)},
		{Type: courierv1alpha1.ConditionCapacityRecovery, Status: metav1.ConditionTrue, Reason: "SECRET-CAPACITY-REASON", LastTransitionTime: metav1.NewTime(testNow)},
		{Type: "UnlistedCondition", Status: metav1.ConditionTrue, Reason: "SECRET-UNLISTED-REASON", LastTransitionTime: metav1.NewTime(testNow)},
	}
	run.Status.Heartbeat = &courierv1alpha1.Heartbeat{
		At: metav1.NewTime(testNow), Kind: "SECRET-KIND", CoordinatorPodUID: "SECRET-POD-UID",
	}
	run.Status.TerminalDiagnostic = &courierv1alpha1.TerminalDiagnostic{
		Category:   courierv1alpha1.TerminalDiagnosticCategoryKubernetesFailure,
		ExitCode:   int32p(137),
		Reason:     "plain reason",
		ObservedAt: ptrTime(metav1.NewTime(testNow)),
		Container: &courierv1alpha1.TerminalContainerDiagnostic{
			PodName:           "SECRET-POD-NAME",
			PodUID:            "SECRET-POD-UID-2",
			ContainerID:       "containerd://SECRET-CONTAINER-ID",
			Image:             "SECRET-IMAGE",
			NodeName:          "SECRET-NODE",
			ContainerName:     "coordinator",
			RestartCount:      3,
			TerminationReason: "OOMKilled",
		},
	}
	_, handler := serverFixture(t, &run)

	detailRec := get(handler, "/v1/runs/run")
	if detailRec.Code != http.StatusOK {
		t.Fatalf("run detail = %d, want 200 (%s)", detailRec.Code, detailRec.Body.String())
	}
	listRec := get(handler, PathRuns)
	overviewRec := get(handler, PathOverview)
	for _, rec := range []*httptest.ResponseRecorder{detailRec, listRec, overviewRec} {
		body := rec.Body.String()
		for _, secret := range []string{
			"SECRET-SOURCE-CONTEXT", "SECRET-SOURCE-RECEIPT-TOKEN", "SECRET-PLAN-TEXT",
			"SECRET-BRIEF-SUMMARY", "SECRET-LAST-RETRY-TOKEN", "SECRET-ACTIVE-RETRY-TOKEN",
			"SECRET-CHECK-FINGERPRINT", "SECRET-FAILED-CHECK-FINGERPRINT",
			"SECRET-KIND", "SECRET-POD-NAME", "SECRET-POD-UID", "SECRET-CONTAINER-ID",
			"SECRET-IMAGE", "SECRET-NODE",
			// Condition reasons are free text written along reconcile paths and
			// never cross, whatever their condition type or content.
			"SECRET-LIFECYCLE-REASON", "ghp_AAAAABBBBBCCCCCDDDDDEEEEE",
			"SECRET-CAPACITY-REASON", "SECRET-UNLISTED-REASON",
			`"conditionReason"`,
		} {
			if strings.Contains(body, secret) {
				t.Errorf("redaction leak: %q appears in a response body", secret)
			}
		}
	}
	if !strings.Contains(detailRec.Body.String(), `"terminationReason":"OOMKilled"`) {
		t.Error("expected bounded keepers to survive the round trip")
	}
	if !strings.Contains(detailRec.Body.String(), `"type":"`+courierv1alpha1.ConditionLifecycleReported+`"`) {
		t.Error("expected the allowlisted condition itself to survive the round trip")
	}
}

// TestServerNamespaceBoundary pins the namespace scope: a run outside the
// configured namespace is never served, in the list, the totals, or the
// detail lookup.
func TestServerNamespaceBoundary(t *testing.T) {
	inside := runFixture("inside", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	outside := runFixture("outside", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	outside.Namespace = "somewhere-else"
	_, handler := serverFixture(t, &inside, &outside)

	envelope, _ := envelopeOf(t, get(handler, PathRuns))
	var list RunSummaryList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if list.Pagination.Total != 1 {
		t.Errorf("total = %d, want only the in-namespace run", list.Pagination.Total)
	}
	for _, item := range list.Items {
		if item.Name == "outside" || item.Namespace != "courier" {
			t.Errorf("out-of-namespace run served: %+v", item)
		}
	}
	if rec := get(handler, "/v1/runs/outside"); rec.Code != http.StatusNotFound {
		t.Errorf("out-of-namespace run detail = %d, want 404", rec.Code)
	}
	envelope, _ = envelopeOf(t, get(handler, PathOverview))
	var overview Overview
	if err := json.Unmarshal(envelope.Payload, &overview); err != nil {
		t.Fatalf("overview payload: %v", err)
	}
	if overview.Runs.Total != 1 {
		t.Errorf("overview totals = %d runs, want only the in-namespace run", overview.Runs.Total)
	}
}

// TestServerLaneProfilesStayNamespaceScoped pins the lane view's namespace
// scope: the operator resolves a run's lane profile in the run's own
// namespace, so a LaneProfile in another namespace governs nothing and must
// not stand in for a missing one. A lane whose profile is absent from the
// served namespace reads Unsupported, exactly as the operator's admission
// sees it.
func TestServerLaneProfilesStayNamespaceScoped(t *testing.T) {
	run := runFixture("run", "github-label", "cloud", courierv1alpha1.PhaseRunning, time.Now())
	foreign := laneFixture("cloud", 3)
	foreign.Namespace = "courier-system"

	_, handler := serverFixture(t, &run, &foreign)
	envelope, _ := envelopeOf(t, get(handler, PathLanes))
	var list LaneHealthList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("lanes = %d, want 1", len(list.Items))
	}
	if list.Items[0].Availability != AvailabilityUnsupported {
		t.Errorf("lane availability = %q, want Unsupported", list.Items[0].Availability)
	}
	if list.Items[0].ConfiguredConcurrency != 0 {
		t.Errorf("configured concurrency = %d, want 0 (no profile in the served namespace)",
			list.Items[0].ConfiguredConcurrency)
	}

	// The profile in the served namespace makes the same lane configured.
	local := laneFixture("cloud", 3)
	local.Namespace = "courier"
	_, handler = serverFixture(t, &run, &local)
	envelope, _ = envelopeOf(t, get(handler, PathLanes))
	var localList LaneHealthList
	if err := json.Unmarshal(envelope.Payload, &localList); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if len(localList.Items) != 1 || localList.Items[0].Availability != AvailabilityHealthy {
		t.Errorf("lane with its profile = %+v, want Healthy", localList.Items)
	}
	if localList.Items[0].ConfiguredConcurrency != 3 {
		t.Errorf("configured concurrency = %d, want 3", localList.Items[0].ConfiguredConcurrency)
	}
}

// TestServerSourcesRoute pins the per-source health route: one entry per
// source visible in the served namespace's runs, sorted by name, with
// nothing from another namespace.
func TestServerSourcesRoute(t *testing.T) {
	inside := runFixture("inside", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	other := runFixture("other", "cron", "local", courierv1alpha1.PhaseRunning, testNow)
	outside := runFixture("outside", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	outside.Namespace = "somewhere-else"
	_, handler := serverFixture(t, &inside, &other, &outside)

	rec := get(handler, PathSources)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	envelope, _ := envelopeOf(t, rec)
	if envelope.Kind != KindSourceList {
		t.Errorf("kind = %q, want %q", envelope.Kind, KindSourceList)
	}
	var list SourceHealthList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("sources = %d, want 2", len(list.Items))
	}
	if list.Items[0].Name != "cron" || list.Items[1].Name != "github-label" {
		t.Errorf("source names = %q/%q, want cron/github-label sorted", list.Items[0].Name, list.Items[1].Name)
	}
	for _, item := range list.Items {
		if item.TotalRuns != 1 {
			t.Errorf("source %q totalRuns = %d, want 1", item.Name, item.TotalRuns)
		}
	}
}

// TestServerConfiguredIdleSourceProjectedEmpty pins #839: with the wiring's
// source inventory, a configured source adapter with zero CoderRuns appears
// on GET /v1/sources as Empty instead of being absent, and the overview's
// source counts describe the same set. Freshness for an idle source is
// unknown: its absence of observation is named, never a silent zero, and
// discovery state is not cluster state the read model can surface.
func TestServerConfiguredIdleSourceProjectedEmpty(t *testing.T) {
	observed := runFixture("observed", "github-label", "local", courierv1alpha1.PhaseDone, testNow.Add(-time.Minute))
	known := []string{"dispatch", "github-label", "idle-configured-source", "manual"}
	_, handler := serverFixtureWithSources(t, known, &observed)

	rec := get(handler, PathSources)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	envelope, _ := envelopeOf(t, rec)
	var list SourceHealthList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if len(list.Items) != 4 || list.Pagination.Total != 4 {
		t.Fatalf("sources = %+v, want 4 entries", list.Items)
	}
	wantNames := []string{"dispatch", "github-label", "idle-configured-source", "manual"}
	for i, want := range wantNames {
		if list.Items[i].Name != want {
			t.Fatalf("source %d = %q, want %q (sorted)", i, list.Items[i].Name, want)
		}
	}
	for _, item := range list.Items {
		switch item.Name {
		case "github-label":
			if item.TotalRuns != 1 || item.Availability != AvailabilityHealthy {
				t.Errorf("observed source = %+v, want 1 run and Healthy", item)
			}
		default:
			if item.Availability != AvailabilityEmpty {
				t.Errorf("idle source %q availability = %q, want Empty", item.Name, item.Availability)
			}
			if item.TotalRuns != 0 || item.ObservedAt != nil || item.Freshness != FreshnessUnknown {
				t.Errorf("idle source %q = %+v, want zero runs, no observation, unknown freshness", item.Name, item)
			}
			if item.LastSuccessAt != nil || item.LastErrorAt != nil {
				t.Errorf("idle source %q = success %v error %v, want no recorded outcome", item.Name, item.LastSuccessAt, item.LastErrorAt)
			}
		}
	}

	// The overview counts the same source set, so the two views agree.
	envelope, _ = envelopeOf(t, get(handler, PathOverview))
	var overview Overview
	if err := json.Unmarshal(envelope.Payload, &overview); err != nil {
		t.Fatalf("overview payload: %v", err)
	}
	if overview.Sources.Count != 4 || overview.Sources.Healthy != 1 || overview.Sources.Empty != 3 {
		t.Errorf("overview sources = %+v, want count 4, healthy 1, empty 3", overview.Sources)
	}
}

// TestServerWithoutInventoryStaysRunObserved pins the degradation path: a
// server wired without a source inventory — either no KnownSources provider
// at all, or one that returns nothing — keeps the run-observed view, and a
// source name that no run carries stays absent from both source views.
func TestServerWithoutInventoryStaysRunObserved(t *testing.T) {
	observed := runFixture("observed", "github-label", "local", courierv1alpha1.PhaseDone, testNow.Add(-time.Minute))
	_, noProvider := serverFixture(t, &observed)
	_, emptyProvider := serverFixtureWithSources(t, nil, &observed)
	for name, handler := range map[string]http.Handler{
		"no provider":       noProvider,
		"provider is empty": emptyProvider,
	} {
		for _, path := range []string{PathSources, PathOverview} {
			rec := get(handler, path)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s [%s] status = %d, want 200", name, path, rec.Code)
			}
			if body := rec.Body.String(); strings.Contains(body, "idle-configured-source") {
				t.Errorf("%s [%s] lists a source no run carries without an inventory: %s", name, path, body)
			}
		}
		envelope, _ := envelopeOf(t, get(handler, PathSources))
		var list SourceHealthList
		if err := json.Unmarshal(envelope.Payload, &list); err != nil {
			t.Fatalf("%s payload: %v", name, err)
		}
		if len(list.Items) != 1 || list.Items[0].Name != "github-label" {
			t.Errorf("%s sources = %+v, want only the run-observed github-label", name, list.Items)
		}
	}
}

// TestServerBoundedListWalk pins the page-walked namespace scan: every List
// round trip carries the server's page limit, the continuation token is
// followed until the collection is exhausted, and no single call asks the
// API server for an unbounded collection. The fake client does not emulate
// pagination, so the pages are fabricated behind the interceptor: this
// test pins the walk logic, not the API server's paging behavior itself.
func TestServerBoundedListWalk(t *testing.T) {
	const total = ListPageSize + 120
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	allRuns := make([]courierv1alpha1.CoderRun, 0, total)
	base := testNow.Add(-time.Duration(total) * time.Minute)
	for i := 0; i < total; i++ {
		allRuns = append(allRuns, runFixture(fmt.Sprintf("run-%03d", i), "github-label", "local", courierv1alpha1.PhaseRunning, base.Add(time.Duration(i)*time.Minute)))
	}

	type listCall struct {
		limit int64
		cont  string
	}
	var calls []listCall
	pageStart := 0
	reader := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).Build(), interceptor.Funcs{
		List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
			var applied ctrlclient.ListOptions
			for _, opt := range opts {
				opt.ApplyToList(&applied)
			}
			calls = append(calls, listCall{limit: applied.Limit, cont: applied.Continue})
			typed, ok := list.(*courierv1alpha1.CoderRunList)
			if !ok {
				return c.List(ctx, list, opts...)
			}
			end := pageStart + int(applied.Limit)
			if applied.Limit <= 0 || end > total {
				end = total
			}
			typed.Items = allRuns[pageStart:end]
			if end < total {
				typed.Continue = fmt.Sprintf("page-%d", end)
			}
			pageStart = end
			return nil
		},
	})
	server, err := NewServer(ServerConfig{Reader: reader, Token: testToken, Bind: ":0", Namespace: "courier"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	handler := server.Handler()

	envelope, body := envelopeOf(t, get(handler, PathRuns))
	var list RunSummaryList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v\n%s", err, body)
	}
	if len(list.Items) != DefaultPageSize {
		t.Errorf("first page = %d items, want %d", len(list.Items), DefaultPageSize)
	}
	if list.Pagination.Total != total {
		t.Errorf("total = %d, want the full walked count %d", list.Pagination.Total, total)
	}
	if list.Pagination.Truncated {
		t.Error("truncated = true for an exhausted scan")
	}
	if len(calls) != 2 {
		t.Fatalf("scan made %d List calls, want 2 pages for %d items", len(calls), total)
	}
	// The walk must resume with the exact token the previous page handed
	// out, never just some non-empty string.
	wantContinue := ""
	for i, call := range calls {
		if call.limit != ListPageSize {
			t.Errorf("List call %d limit = %d, want %d", i, call.limit, ListPageSize)
		}
		if call.cont != wantContinue {
			t.Errorf("List call %d continuation = %q, want %q", i, call.cont, wantContinue)
		}
		wantContinue = fmt.Sprintf("page-%d", ListPageSize)
	}
}

// TestServerFetchCapTruncates pins the hard fetch cap against a dataset
// larger than the cap: the walk stops at MaxListItems objects no matter how
// much more the collection holds, and every projection that consumed the
// capped window states the truncation explicitly.
func TestServerFetchCapTruncates(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	inner := fake.NewClientBuilder().WithScheme(scheme).Build()

	// The fabricated reader never runs dry: every page is full and carries
	// a continuation token, modelling a collection larger than the cap.
	var runListCalls int
	reader := interceptor.NewClient(inner, interceptor.Funcs{
		List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
			switch typed := list.(type) {
			case *courierv1alpha1.CoderRunList:
				runListCalls++
				for i := 0; i < ListPageSize; i++ {
					run := runFixture(fmt.Sprintf("run-%d-%03d", runListCalls, i), "github-label", "local", courierv1alpha1.PhaseRunning, testNow.Add(-time.Duration(i)*time.Minute))
					typed.Items = append(typed.Items, run)
				}
				typed.Continue = "more"
			case *courierv1alpha1.LaneProfileList:
				lane := laneFixture("local", 1)
				typed.Items = append(typed.Items, lane)
			default:
				return c.List(ctx, list, opts...)
			}
			return nil
		},
	})
	server, err := NewServer(ServerConfig{Reader: reader, Token: testToken, Bind: ":0", Namespace: "courier"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	handler := server.Handler()

	// The cap is reached in ceil(MaxListItems/ListPageSize) full pages.
	wantCalls := (MaxListItems + ListPageSize - 1) / ListPageSize

	envelope, body := envelopeOf(t, get(handler, PathRuns))
	var list RunSummaryList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v\n%s", err, body)
	}
	if !list.Pagination.Truncated {
		t.Error("capped scan must state truncation")
	}
	if list.Pagination.Total > MaxListItems {
		t.Errorf("total = %d, want at most the cap %d", list.Pagination.Total, MaxListItems)
	}
	if len(list.Items) != DefaultPageSize {
		t.Errorf("page = %d items, want %d", len(list.Items), MaxPageSize)
	}
	if runListCalls != wantCalls {
		t.Errorf("run scan made %d List calls, want %d — the walk must stop at the cap", runListCalls, wantCalls)
	}

	runListCalls = 0
	envelope, body = envelopeOf(t, get(handler, PathOverview))
	var overview Overview
	if err := json.Unmarshal(envelope.Payload, &overview); err != nil {
		t.Fatalf("overview payload: %v\n%s", err, body)
	}
	if !overview.Truncated {
		t.Error("overview over a capped window must state truncation")
	}
	if overview.Runs.Total > MaxListItems {
		t.Errorf("overview totals %d runs, want at most the cap %d", overview.Runs.Total, MaxListItems)
	}
	if runListCalls != wantCalls {
		t.Errorf("overview run scan made %d List calls, want %d", runListCalls, wantCalls)
	}

	envelope, body = envelopeOf(t, get(handler, PathLanes))
	var lanes LaneHealthList
	if err := json.Unmarshal(envelope.Payload, &lanes); err != nil {
		t.Fatalf("lanes payload: %v\n%s", err, body)
	}
	if !lanes.Pagination.Truncated {
		t.Error("lane health over a capped run window must state truncation")
	}
}

// TestServerEmptyPageWalkStopsAtRoundTripCap pins the round-trip bound: an
// API server handing back empty pages with a live continuation token cannot
// drive an unbounded walk; the pass stops at MaxListRoundTrips and reports
// the window truncated instead of looping to the deadline.
func TestServerEmptyPageWalkStopsAtRoundTripCap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	var runListCalls int
	reader := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).Build(), interceptor.Funcs{
		List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
			if typed, ok := list.(*courierv1alpha1.CoderRunList); ok {
				runListCalls++
				typed.Continue = "still-more"
				return nil
			}
			return c.List(ctx, list, opts...)
		},
	})
	server, err := NewServer(ServerConfig{Reader: reader, Token: testToken, Bind: ":0", Namespace: "courier"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	rec := get(server.Handler(), PathRuns)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET runs = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	envelope, body := envelopeOf(t, rec)
	var list RunSummaryList
	if err := json.Unmarshal(envelope.Payload, &list); err != nil {
		t.Fatalf("payload: %v\n%s", err, body)
	}
	if !list.Pagination.Truncated {
		t.Error("a walk cut at the round-trip bound must state truncation")
	}
	if len(list.Items) != 0 {
		t.Errorf("items = %d, want an empty window", len(list.Items))
	}
	if runListCalls != MaxListRoundTrips {
		t.Errorf("run scan made %d List calls, want exactly the bound %d", runListCalls, MaxListRoundTrips)
	}
}

// TestServerRunDetailErrorMapping pins the detail lookup's error branches:
// an invalid run name is a 404 before any cluster contact, and a Get
// failure that is not a 404 is a 500 with the fixed internal message.
func TestServerRunDetailErrorMapping(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	var getCalls int
	reader := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).Build(), interceptor.Funcs{
		Get: func(ctx context.Context, c ctrlclient.WithWatch, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
			getCalls++
			return fmt.Errorf("cluster said no")
		},
	})
	server, err := NewServer(ServerConfig{Reader: reader, Token: testToken, Bind: ":0", Namespace: "courier"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	handler := server.Handler()

	if rec := get(handler, "/v1/runs/not%20valid"); rec.Code != http.StatusNotFound {
		t.Errorf("invalid run name = %d, want 404", rec.Code)
	}
	if getCalls != 0 {
		t.Errorf("invalid name made %d cluster Gets, want none", getCalls)
	}
	rec := get(handler, "/v1/runs/run")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Get failure = %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "cluster said no") {
		t.Errorf("raw cluster error text crossed the surface: %s", rec.Body.String())
	}
}

// TestServerReadBudgetShedsWith503 pins the concurrent-scan budget: with
// every scan slot held, an additional scan is shed with an immediate 503
// rather than queueing, and held requests still complete.
func TestServerReadBudgetShedsWith503(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	run := runFixture("run", "github-label", "local", courierv1alpha1.PhaseRunning, testNow)
	inner := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&run).Build()

	release := make(chan struct{})
	entered := make(chan struct{}, 2*MaxConcurrentReads)
	reader := interceptor.NewClient(inner, interceptor.Funcs{
		List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
			entered <- struct{}{}
			<-release
			return c.List(ctx, list, opts...)
		},
	})
	server, err := NewServer(ServerConfig{Reader: reader, Token: testToken, Bind: ":0", Namespace: "courier"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	handler := server.Handler()

	codes := make(chan int, MaxConcurrentReads)
	for i := 0; i < MaxConcurrentReads; i++ {
		go func() { codes <- get(handler, PathOverview).Code }()
	}
	for i := 0; i < MaxConcurrentReads; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("held scan never reached the reader")
		}
	}

	if rec := get(handler, PathRuns); rec.Code != http.StatusServiceUnavailable {
		close(release)
		t.Errorf("scan beyond the budget = %d, want 503", rec.Code)
	}
	close(release)
	for i := 0; i < MaxConcurrentReads; i++ {
		select {
		case code := <-codes:
			if code != http.StatusOK {
				t.Errorf("held request = %d, want 200", code)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("held requests never completed")
		}
	}
}

// toObjects widens fixtures to the fake client's object slice.
func toObjects(runs []*courierv1alpha1.CoderRun) []ctrlclient.Object {
	objs := make([]ctrlclient.Object, 0, len(runs))
	for _, run := range runs {
		objs = append(objs, run)
	}
	return objs
}
