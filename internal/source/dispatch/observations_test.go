package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

// testObservation is one fully-populated execution observation: a run in a
// non-UTC location, a mapped work mode, and a pull request URL, so the
// contract tests exercise every shape the body can carry.
func testObservation() source.ExecutionObservation {
	return source.ExecutionObservation{
		RunUID:       "run-uid-1",
		RunNamespace: "team-a",
		RunName:      "coder-run-1",
		WorkItemID:   "opaque-work-1",
		WorkIdentity: "followup-pr/acme/widgets#7/item-7@1",
		Mode:         "resolve-issue",
		Source:       "dispatch",
		State:        source.ExecutionSucceeded,
		ObservedAt:   time.Date(2026, 10, 29, 14, 30, 45, 0, time.FixedZone("UTC+2", 7200)),
		PR:           "https://github.com/acme/widgets/pull/7",
	}
}

// assertObservationCapability checks the capability catalogue gauge for the
// execution-observations capability: exactly the given state is present, and
// no other state's series survives it.
func assertObservationCapability(t *testing.T, reg *prometheus.Registry, state string) {
	t.Helper()
	labels := map[string]string{
		"connector":  defaultConnectorName,
		"capability": connector.CapabilityExecutionObservations,
		"state":      state,
	}
	got, ok := gaugeValue(t, reg, connector.MetricCapabilityInfo, labels)
	if !ok || got != 1 {
		t.Fatalf("capability gauge state %q = %v present=%v, want 1", state, got, ok)
	}
	for _, other := range connector.CapabilityStates {
		if other == state {
			continue
		}
		if _, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
			"connector":  defaultConnectorName,
			"capability": connector.CapabilityExecutionObservations,
			"state":      other,
		}); ok {
			t.Fatalf("capability gauge state %q must not survive alongside %q", other, state)
		}
	}
}

// observationCapture records the wire state of one observations POST.
type observationCapture struct {
	method string
	path   string
	auth   string
	body   map[string]any
}

// TestObservationContractGuard fixes the wire contract of the
// execution-observation route (misospace/courier#633): the request shape and
// the body spelling are a contract with the Dispatch server, so the tests
// assert exact values.
func TestObservationContractGuard(t *testing.T) {
	var got observationCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = observationCapture{method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization")}
		if err := json.NewDecoder(r.Body).Decode(&got.body); err != nil {
			t.Errorf("decode observations body: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "worker", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	obs := testObservation()
	observedAt := obs.ObservedAt
	want := map[string]any{
		"version":           float64(1),
		"idempotencyKey":    fmt.Sprintf("execution/run-uid-1/succeeded/%d", observedAt.UTC().UnixMilli()),
		"agent":             "worker",
		"source":            "dispatch",
		"run":               map[string]any{"uid": "run-uid-1", "namespace": "team-a", "name": "coder-run-1"},
		"work":              map[string]any{"itemId": "opaque-work-1", "identity": "followup-pr/acme/widgets#7/item-7@1", "mode": "resolve-issue", "type": "implement"},
		"state":             "succeeded",
		"observedAt":        "2026-10-29T12:30:45Z",
		"pullRequestNumber": float64(7),
		"pullRequestUrl":    "https://github.com/acme/widgets/pull/7",
	}
	if err := client.ReportExecution(ctx, obs); err != nil {
		t.Fatalf("ReportExecution() error = %v", err)
	}
	if got.method != http.MethodPost || got.path != "/api/agent-work/observations" {
		t.Errorf("request = %s %s, want POST /api/agent-work/observations", got.method, got.path)
	}
	if got.auth != "Bearer secret-token" {
		t.Errorf("Authorization = %q, want %q", got.auth, "Bearer secret-token")
	}
	if !reflect.DeepEqual(got.body, want) {
		t.Errorf("body = %#v, want %#v", got.body, want)
	}

	workShapes := []struct {
		name     string
		mode     string
		wantWork map[string]any
	}{
		{
			name:     "resolve-issue maps to implement",
			mode:     "resolve-issue",
			wantWork: map[string]any{"itemId": "opaque-work-1", "identity": "followup-pr/acme/widgets#7/item-7@1", "mode": "resolve-issue", "type": "implement"},
		},
		{
			name:     "fix-pr maps to followup-pr",
			mode:     "fix-pr",
			wantWork: map[string]any{"itemId": "opaque-work-1", "identity": "followup-pr/acme/widgets#7/item-7@1", "mode": "fix-pr", "type": "followup-pr"},
		},
		{
			name:     "unknown mode carries no type",
			mode:     "rebase",
			wantWork: map[string]any{"itemId": "opaque-work-1", "identity": "followup-pr/acme/widgets#7/item-7@1", "mode": "rebase"},
		},
	}
	for _, tc := range workShapes {
		t.Run(tc.name, func(t *testing.T) {
			obs := testObservation()
			obs.Mode = tc.mode
			obs.PR = ""
			if err := client.ReportExecution(ctx, obs); err != nil {
				t.Fatalf("ReportExecution() error = %v", err)
			}
			if !reflect.DeepEqual(got.body["work"], tc.wantWork) {
				t.Errorf("work = %#v, want %#v", got.body["work"], tc.wantWork)
			}
			if _, present := got.body["pullRequestNumber"]; present {
				t.Error("pullRequestNumber must be absent without a pull request")
			}
			if _, present := got.body["pullRequestUrl"]; present {
				t.Error("pullRequestUrl must be absent without a pull request")
			}
		})
	}

	prShapes := []struct {
		name       string
		pr         string
		wantNumber bool
		wantURL    bool
	}{
		{name: "absent omits both keys", pr: ""},
		{name: "number only carries no url", pr: "77", wantNumber: true},
		{name: "url carries number and url", pr: "https://github.com/acme/widgets/pull/7", wantNumber: true, wantURL: true},
		{name: "unparseable omits both keys without error", pr: "not-a-pr"},
	}
	for _, tc := range prShapes {
		t.Run(tc.name, func(t *testing.T) {
			obs := testObservation()
			obs.PR = tc.pr
			if err := client.ReportExecution(ctx, obs); err != nil {
				t.Fatalf("ReportExecution() error = %v", err)
			}
			if _, present := got.body["pullRequestNumber"]; present != tc.wantNumber {
				t.Errorf("pullRequestNumber present = %v, want %v", present, tc.wantNumber)
			}
			if _, present := got.body["pullRequestUrl"]; present != tc.wantURL {
				t.Errorf("pullRequestUrl present = %v, want %v", present, tc.wantURL)
			}
		})
	}
}

// TestObservationLocalValidation fixes the client's pre-flight check: an
// unaddressable observation fails before any HTTP round trip.
func TestObservationLocalValidation(t *testing.T) {
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "worker", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	obs := testObservation()
	obs.RunUID = ""
	if err := client.ReportExecution(ctx, obs); err == nil {
		t.Fatal("ReportExecution() without a run UID = nil, want a validation failure")
	}
	obs = testObservation()
	obs.WorkItemID = ""
	if err := client.ReportExecution(ctx, obs); err == nil {
		t.Fatal("ReportExecution() without a work item ID = nil, want a validation failure")
	}
	if hits != 0 {
		t.Fatalf("observations round trips = %d, want 0 for unaddressable input", hits)
	}
}

// TestObservationIdempotencyKeyStability fixes the delivery idempotency key:
// byte-identical re-sends of the same observation share the key, and a
// changed observation time yields a different one. Restart convergence is
// not this key's job: it comes from the source keying current state by
// (RunUID, State) as an upsert.
func TestObservationIdempotencyKeyStability(t *testing.T) {
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode observations body: %v", err)
		}
		keys = append(keys, fmt.Sprint(body["idempotencyKey"]))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "worker", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	obs := testObservation()
	if err := client.ReportExecution(ctx, obs); err != nil {
		t.Fatal(err)
	}
	if err := client.ReportExecution(ctx, obs); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("idempotency keys for the same observation = %v, want two equal keys", keys)
	}
	obs.ObservedAt = obs.ObservedAt.Add(time.Millisecond)
	if err := client.ReportExecution(ctx, obs); err != nil {
		t.Fatal(err)
	}
	if keys[2] == keys[0] {
		t.Fatalf("idempotency key %q must change when the observation time changes", keys[2])
	}
}

// TestObservationLatchesUnsupportedRoute fixes the version-skew latch: a bare
// 404/405/501 on the route proves the deployment has not adopted it, so the
// client latches unsupported for the process, the capability gauge flips, and
// every further report short-circuits without an HTTP round trip.
func TestObservationLatchesUnsupportedRoute(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented} {
		t.Run(fmt.Sprintf("HTTP_%d", status), func(t *testing.T) {
			var hits int
			f := newDiagnosticsFixture(t, nil, func(f *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
				hits++
				w.WriteHeader(status)
			})
			obs := testObservation()
			err := f.client.ReportExecution(context.Background(), obs)
			if !errors.Is(err, source.ErrUnsupportedExecutionObservations) {
				t.Fatalf("ReportExecution() error = %v, want %v", err, source.ErrUnsupportedExecutionObservations)
			}
			assertObservationCapability(t, f.reg, connector.CapabilityUnsupported)
			if err := f.client.ReportExecution(context.Background(), obs); !errors.Is(err, source.ErrUnsupportedExecutionObservations) {
				t.Fatalf("second ReportExecution() error = %v, want %v", err, source.ErrUnsupportedExecutionObservations)
			}
			if hits != 1 {
				t.Fatalf("observations round trips = %d, want 1 (the latch must short-circuit)", hits)
			}
		})
	}
}

// failingDoer is an HTTP transport that fails every request, standing in for
// a network failure short of a server response.
type failingDoer struct{}

func (failingDoer) Do(*http.Request) (*http.Response, error) {
	return nil, errors.New("connection refused")
}

// TestObservationNonLatchingFailures fixes the other side of the latch:
// authentication failures, rate limits, 5xx, and transport failures stay
// distinct retryable classes — they never read as "unsupported", never latch,
// and a later 2xx against a fixed endpoint still succeeds.
func TestObservationNonLatchingFailures(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusConflict,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	} {
		t.Run(fmt.Sprintf("HTTP_%d", status), func(t *testing.T) {
			code := status
			f := newDiagnosticsFixture(t, nil, func(f *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			})
			obs := testObservation()
			err := f.client.ReportExecution(context.Background(), obs)
			if err == nil {
				t.Fatal("ReportExecution() error = nil, want a failure")
			}
			if errors.Is(err, source.ErrUnsupportedExecutionObservations) {
				t.Fatalf("failure %v must not read as unsupported", err)
			}
			assertObservationCapability(t, f.reg, connector.CapabilitySupported)
			code = http.StatusNoContent
			if err := f.client.ReportExecution(context.Background(), obs); err != nil {
				t.Fatalf("ReportExecution() after the endpoint recovered = %v, want nil", err)
			}
			assertObservationCapability(t, f.reg, connector.CapabilitySupported)
		})
	}

	t.Run("transport-failure", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, func(f *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
		original := f.client.httpClient
		f.client.httpClient = failingDoer{}
		obs := testObservation()
		err := f.client.ReportExecution(context.Background(), obs)
		if err == nil {
			t.Fatal("ReportExecution() error = nil, want a transport failure")
		}
		if errors.Is(err, source.ErrUnsupportedExecutionObservations) {
			t.Fatalf("failure %v must not read as unsupported", err)
		}
		assertObservationCapability(t, f.reg, connector.CapabilitySupported)
		f.client.httpClient = original
		if err := f.client.ReportExecution(context.Background(), obs); err != nil {
			t.Fatalf("ReportExecution() after the transport recovered = %v, want nil", err)
		}
		assertObservationCapability(t, f.reg, connector.CapabilitySupported)
	})
}

// observingFakeClient adds the optional execution-observation seam to the
// minimal client contract, recording exactly what the adapter forwards.
type observingFakeClient struct {
	basicClient
	observations []source.ExecutionObservation
}

func (c *observingFakeClient) ReportExecution(_ context.Context, obs source.ExecutionObservation) error {
	c.observations = append(c.observations, obs)
	return nil
}

// TestAdapterReportExecutionDegradation fixes the adapter boundary: a client
// without the observation seam reports the permanent unsupported posture
// instead of emulating the report, a client with it forwards untouched, and
// the capability gauge follows the seam.
func TestAdapterReportExecutionDegradation(t *testing.T) {
	t.Run("client without seam", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		adapter := New(&basicClient{}, WithConnectorDiagnostics(connector.NewEmitter(reg, nil)))
		err := adapter.ReportExecution(context.Background(), testObservation())
		if !errors.Is(err, source.ErrUnsupportedExecutionObservations) {
			t.Fatalf("ReportExecution() error = %v, want %v", err, source.ErrUnsupportedExecutionObservations)
		}
		assertObservationCapability(t, reg, connector.CapabilityUnsupported)
	})

	t.Run("client with seam", func(t *testing.T) {
		observing := &observingFakeClient{}
		reg := prometheus.NewRegistry()
		adapter := New(observing, WithConnectorDiagnostics(connector.NewEmitter(reg, nil)))
		obs := testObservation()
		if err := adapter.ReportExecution(context.Background(), obs); err != nil {
			t.Fatalf("ReportExecution() error = %v", err)
		}
		if len(observing.observations) != 1 || !reflect.DeepEqual(observing.observations[0], obs) {
			t.Fatalf("forwarded observations = %+v, want one unaltered copy of %+v", observing.observations, obs)
		}
		assertObservationCapability(t, reg, connector.CapabilitySupported)
	})

	t.Run("empty work item", func(t *testing.T) {
		observing := &observingFakeClient{}
		adapter := New(observing)
		obs := testObservation()
		obs.WorkItemID = ""
		err := adapter.ReportExecution(context.Background(), obs)
		if !errors.Is(err, ErrEmptyWorkItemID) {
			t.Fatalf("ReportExecution() error = %v, want %v", err, ErrEmptyWorkItemID)
		}
		if len(observing.observations) != 0 {
			t.Fatalf("forwarded observations = %+v, want none for an unaddressable observation", observing.observations)
		}
	})
}
