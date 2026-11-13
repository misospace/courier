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

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

const testToken = "test-read-token"

// serverFixture builds a disabled-bind server over a fake client holding
// objs, and returns the server and its authenticated handler for
// httptest-driven requests without a listener.
func serverFixture(t *testing.T, objs ...ctrlclient.Object) (*Server, http.Handler) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	server, err := NewServer(ServerConfig{
		Reader:    reader,
		Token:     testToken,
		Bind:      ":0",
		Namespace: "courier",
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

	paths := []string{PathOverview, PathLanes, PathRuns, "/v1/runs/run"}
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
		} {
			if strings.Contains(body, secret) {
				t.Errorf("redaction leak: %q appears in a response body", secret)
			}
		}
	}
	if !strings.Contains(detailRec.Body.String(), `"terminationReason":"OOMKilled"`) {
		t.Error("expected bounded keepers to survive the round trip")
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

// toObjects widens fixtures to the fake client's object slice.
func toObjects(runs []*courierv1alpha1.CoderRun) []ctrlclient.Object {
	objs := make([]ctrlclient.Object, 0, len(runs))
	for _, run := range runs {
		objs = append(objs, run)
	}
	return objs
}
