package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

// diagnosticsFixture wires a Dispatch client and adapter to a fresh
// Prometheus registry through the connector diagnostic envelope.
type diagnosticsFixture struct {
	em      *connector.Emitter
	reg     *prometheus.Registry
	client  *HTTPClient
	adapter *Adapter
	server  *httptest.Server

	excluded [][]string
}

// newDiagnosticsFixture builds the fixture around a handler that receives
// the fixture itself, so handlers can record per-request wire state.
func newDiagnosticsFixture(t *testing.T, configure func(*HTTPClient), handle func(f *diagnosticsFixture, w http.ResponseWriter, r *http.Request)) *diagnosticsFixture {
	t.Helper()
	f := &diagnosticsFixture{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handle(f, w, r)
	}))
	t.Cleanup(f.server.Close)
	f.reg = prometheus.NewRegistry()
	f.em = connector.NewEmitter(f.reg, nil)
	client, err := NewClient(f.server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if configure != nil {
		configure(client)
	}
	f.client = client
	f.adapter = New(client, WithConnectorDiagnostics(f.em))
	return f
}

// nextTaskIdle answers next-task with an idle response, recording the
// exclude_pr_fix values of the request, and optionally carrying an
// exclusion acknowledgement payload.
func nextTaskIdle(ack any) func(*diagnosticsFixture, http.ResponseWriter, *http.Request) {
	return func(f *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
		f.excluded = append(f.excluded, r.URL.Query()["exclude_pr_fix"])
		response := map[string]any{"type": "idle", "shouldRun": false}
		if ack != nil {
			response["prFixExclusions"] = ack
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}

func followupWorkID(number int) string {
	return EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: number},
		PRFixItem:   &PRFixItem{ID: fmt.Sprintf("item-%d", number), Generation: 1},
	})
}

func uniqueExclusions(count int) []source.WorkItem {
	items := make([]source.WorkItem, count)
	for i := range items {
		items[i] = source.WorkItem{ID: followupWorkID(i + 1)}
	}
	return items
}

func findSeries(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (*dto.Metric, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					match = false
					break
				}
			}
			if match {
				return m, true
			}
		}
	}
	return nil, false
}

func counterValue(t *testing.T, reg *prometheus.Registry, labels map[string]string) (float64, bool) {
	t.Helper()
	m, ok := findSeries(t, reg, connector.MetricDiagnosticsTotal, labels)
	if !ok || m.GetCounter() == nil {
		return 0, false
	}
	return m.GetCounter().GetValue(), true
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	m, ok := findSeries(t, reg, name, labels)
	if !ok || m.GetGauge() == nil {
		return 0, false
	}
	return m.GetGauge().GetValue(), true
}

// diagnosticsCounter returns the diagnostics counter for a (signal, reason)
// pair, or -1 when the series does not exist.
func diagnosticsCounter(t *testing.T, f *diagnosticsFixture, signal, reason string) float64 {
	t.Helper()
	got, ok := counterValue(t, f.reg, map[string]string{
		"connector": defaultConnectorName, "signal": signal, "reason": reason,
	})
	if !ok {
		return -1
	}
	return got
}

// gatheredConnectorSeries returns every series of every courier_connector_*
// family with its label pairs.
func gatheredConnectorSeries(t *testing.T, reg *prometheus.Registry) []map[string]string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var series []map[string]string
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "courier_connector_") {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			series = append(series, labels)
		}
	}
	return series
}

// totalProtocolErrors sums every protocol-error diagnostic series.
func totalProtocolErrors(t *testing.T, f *diagnosticsFixture) float64 {
	t.Helper()
	total := 0.0
	for _, series := range gatheredConnectorSeries(t, f.reg) {
		if series["signal"] == connector.SignalProtocolError {
			if got, ok := counterValue(t, f.reg, series); ok {
				total += got
			}
		}
	}
	return total
}

// TestOneOverflowDiagnosticPerExclusionCycle is the one-overflow-per-cycle
// assertion (§11.2): 34 unique valid descriptors against the 32-entry
// contract cap truncate to 32, count the overflow diagnostic exactly once,
// and leave the drop count in the overflow gauge, never in extra events.
func TestOneOverflowDiagnosticPerExclusionCycle(t *testing.T) {
	f := newDiagnosticsFixture(t, func(c *HTTPClient) { c.WithPRFixExclusions(true) }, nextTaskIdle(map[string]any{"version": 1}))
	if _, err := f.client.DiscoverExcluding(context.Background(), uniqueExclusions(34)); err != nil {
		t.Fatal(err)
	}
	if got := diagnosticsCounter(t, f, connector.SignalExclusion, connector.ReasonOverflow); got != 1 {
		t.Fatalf("overflow counter = %v, want exactly 1", got)
	}
	conn := map[string]string{"connector": defaultConnectorName}
	for name, want := range map[string]float64{
		connector.MetricExclusionRequested: 34,
		connector.MetricExclusionAccepted:  32,
		connector.MetricExclusionOverflow:  2,
		connector.MetricExclusionLimit:     32,
		connector.MetricExclusionPressure:  34.0 / 32.0,
	} {
		if got, ok := gaugeValue(t, f.reg, name, conn); !ok || got != want {
			t.Errorf("%s = %v, ok=%v; want %v", name, got, ok, want)
		}
	}
	requested, _ := gaugeValue(t, f.reg, connector.MetricExclusionRequested, conn)
	accepted, _ := gaugeValue(t, f.reg, connector.MetricExclusionAccepted, conn)
	overflow, _ := gaugeValue(t, f.reg, connector.MetricExclusionOverflow, conn)
	if accepted < requested && overflow <= 0 {
		t.Errorf("accepted %v < requested %v but overflow = %v", accepted, requested, overflow)
	}
}

// TestExclusionLimitExportedAndTruncationConfigurable is the configurable
// limit assertion: WithPRFixExclusionLimit(5) truncates at 5 and exports the
// limit gauge as data; the default stays 32 when unset.
func TestExclusionLimitExportedAndTruncationConfigurable(t *testing.T) {
	f := newDiagnosticsFixture(t, func(c *HTTPClient) {
		c.WithPRFixExclusions(true).WithPRFixExclusionLimit(5)
	}, nextTaskIdle(map[string]any{"version": 1}))
	if _, err := f.client.DiscoverExcluding(context.Background(), uniqueExclusions(7)); err != nil {
		t.Fatal(err)
	}
	if got := len(f.excluded[0]); got != 5 {
		t.Fatalf("request carried %d exclusions, want 5", got)
	}
	conn := map[string]string{"connector": defaultConnectorName}
	for name, want := range map[string]float64{
		connector.MetricExclusionLimit:     5,
		connector.MetricExclusionRequested: 7,
		connector.MetricExclusionAccepted:  5,
		connector.MetricExclusionOverflow:  2,
	} {
		if got, ok := gaugeValue(t, f.reg, name, conn); !ok || got != want {
			t.Errorf("%s = %v, ok=%v; want %v", name, got, ok, want)
		}
	}
	if got := diagnosticsCounter(t, f, connector.SignalExclusion, connector.ReasonOverflow); got != 1 {
		t.Errorf("overflow counter = %v, want 1", got)
	}

	// The unset default exports the historical contract maximum, and an
	// invalid limit keeps the default rather than introducing a zero cap.
	for _, tc := range []struct {
		name      string
		configure func(*HTTPClient)
	}{
		{"default", func(c *HTTPClient) { c.WithPRFixExclusions(true) }},
		{"invalid-zero", func(c *HTTPClient) { c.WithPRFixExclusions(true).WithPRFixExclusionLimit(0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDiagnosticsFixture(t, tc.configure, nextTaskIdle(nil))
			if got, ok := gaugeValue(t, f.reg, connector.MetricExclusionLimit, conn); !ok || got != 32 {
				t.Errorf("exclusion limit gauge = %v, ok=%v; want 32", got, ok)
			}
		})
	}
}

// TestAcknowledgementStateDiagnostics fixtures each acknowledgement state
// (§3.2): a well-formed acknowledgement counts ok, and every failure state
// counts its own reason — never a fabricated success.
func TestAcknowledgementStateDiagnostics(t *testing.T) {
	cases := []struct {
		name   string
		ack    any
		reason string
	}{
		{"ok", map[string]any{"version": 1, "skipped": []any{}}, reasonAckOK},
		{"missing", nil, reasonAckMissing},
		{"invalid", "bogus", reasonAckInvalid},
		{"version-unsupported", map[string]any{"version": 2}, reasonAckVersionUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDiagnosticsFixture(t, func(c *HTTPClient) { c.WithPRFixExclusions(true) }, nextTaskIdle(tc.ack))
			_, err := f.client.DiscoverExcluding(context.Background(), uniqueExclusions(1))
			if tc.reason == reasonAckOK {
				if err != nil {
					t.Fatalf("well-formed acknowledgement failed discovery: %v", err)
				}
			} else if err == nil {
				t.Fatal("failed acknowledgement was accepted as success")
			}
			if got := diagnosticsCounter(t, f, connector.SignalAck, tc.reason); got != 1 {
				t.Fatalf("ack/%s counter = %v, want 1", tc.reason, got)
			}
			if tc.reason != reasonAckOK {
				if got := diagnosticsCounter(t, f, connector.SignalAck, reasonAckOK); got != -1 {
					t.Fatalf("failed acknowledgement fabricated an ack/ok count of %v", got)
				}
			}
		})
	}
}

// TestOneProtocolErrorPerFailingPoll is the bounded-retry assertion (§11.2):
// N consecutive failing polls produce exactly N protocol-error diagnostics —
// one per attempt, no coalescing, no fan-out.
func TestOneProtocolErrorPerFailingPoll(t *testing.T) {
	const attempts = 3
	f := newDiagnosticsFixture(t, nil, func(_ *diagnosticsFixture, w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	for i := 0; i < attempts; i++ {
		if _, err := f.client.Discover(context.Background()); err == nil {
			t.Fatal("failing poll unexpectedly succeeded")
		}
	}
	if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonHTTP5xx); got != attempts {
		t.Fatalf("protocol-error/http-5xx counter = %v, want %d", got, attempts)
	}
	if total := totalProtocolErrors(t, f); total != attempts {
		t.Fatalf("total protocol-error diagnostics = %v, want %d (no coalescing, no fan-out)", total, attempts)
	}
	// A 5xx is a reachable-but-failing source, not an unreachable one.
	if got := diagnosticsCounter(t, f, connector.SignalAvailability, connector.AvailabilityDegraded); got != attempts {
		t.Fatalf("availability/degraded counter = %v, want %d", got, attempts)
	}
}

// TestClassifiedFailureCountsOnce is the no-double-count assertion (§11.2):
// one failed poll whose error carries a classification emits exactly one
// protocol-error diagnostic for that poll.
func TestClassifiedFailureCountsOnce(t *testing.T) {
	f := newDiagnosticsFixture(t, nil, func(_ *diagnosticsFixture, w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	})
	if _, err := f.client.Discover(context.Background()); err == nil {
		t.Fatal("rejected poll unexpectedly succeeded")
	}
	if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonAuthError); got != 1 {
		t.Fatalf("protocol-error/auth-error counter = %v, want exactly 1", got)
	}
	if total := totalProtocolErrors(t, f); total != 1 {
		t.Fatalf("total protocol-error diagnostics = %v, want exactly 1 for the poll", total)
	}
}

// TestUndeclaredReasonFoldsToOther is the closed-reason assertion (§11.1)
// from the connector side: a client wired to an emitter that has not
// registered the dispatch vocabulary folds every reason onto the single
// other catch-all, so cardinality cannot grow with upstream data.
func TestUndeclaredReasonFoldsToOther(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithConnectorDiagnostics(em, defaultConnectorName)
	if _, err := client.Discover(context.Background()); err == nil {
		t.Fatal("failing poll unexpectedly succeeded")
	}
	got, ok := counterValue(t, reg, map[string]string{
		"connector": defaultConnectorName, "signal": connector.SignalProtocolError, "reason": connector.ReasonOther,
	})
	if !ok || got != 1 {
		t.Fatalf("unregistered vocabulary counter = %v, ok=%v; want reason=other at 1", got, ok)
	}
	if _, ok := counterValue(t, reg, map[string]string{
		"connector": defaultConnectorName, "signal": connector.SignalProtocolError, "reason": reasonHTTP5xx,
	}); ok {
		t.Fatal("undeclared reason leaked past the envelope boundary")
	}
}

// TestCapabilityCatalogueAndClosedLabels exercises discovery, exclusions,
// reporting, and pre-launch revalidation end to end, then asserts every
// gathered courier_connector_* series stays inside the closed label
// vocabularies (§11.1, §6.1): connector name, signal, reason, state, and
// capability only — no repo names, PR numbers, SHAs, or URLs.
func TestCapabilityCatalogueAndClosedLabels(t *testing.T) {
	var marks int
	f := newDiagnosticsFixture(t, func(c *HTTPClient) {
		c.WithPRFixExclusions(true).WithPullRequestStateChecker(PullRequestStateCheckerFunc(
			func(context.Context, string, int) (PullRequestState, error) {
				return PullRequestState{State: "open"}, nil
			}))
	}, func(f *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/agents/worker/next-task":
			f.excluded = append(f.excluded, r.URL.Query()["exclude_pr_fix"])
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "implement", "shouldRun": true,
				"issue":           map[string]any{"id": "issue-1", "repoFullName": "acme/widgets", "number": 42},
				"prFixExclusions": map[string]any{"version": 1},
			})
		case strings.HasSuffix(r.URL.Path, "/tasks/report"):
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		case r.URL.Path == "/api/pr-fix-queue/mark":
			marks++
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})

	// Discovery with an overflowing exclusion set, then the identity and
	// report paths over the offered work.
	items, err := f.client.DiscoverExcluding(context.Background(), uniqueExclusions(34))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want the offered implement task", len(items))
	}
	if err := f.client.Report(context.Background(), items[0].ID, source.Lifecycle{Result: source.ResultReady, PR: "https://github.com/acme/widgets/pull/7"}); err != nil {
		t.Fatal(err)
	}
	if err := f.client.PreLaunch(context.Background(), followupWorkID(7)); err != nil {
		t.Fatal(err)
	}
	if marks != 0 {
		t.Fatalf("mark calls = %d, want 0 (an open pull request revalidates current)", marks)
	}

	declaredReasons := reasonVocabulary()
	closedValues := map[string][]string{
		"connector":  {defaultConnectorName},
		"signal":     append(slices.Clone(connector.Signals), connector.ReasonOther),
		"state":      slices.Concat(connector.CapabilityStates, connector.AvailabilityStates),
		"capability": connector.Capabilities,
	}
	for _, series := range gatheredConnectorSeries(t, f.reg) {
		for key, value := range series {
			switch key {
			case "reason":
				reasons, ok := declaredReasons[series["signal"]]
				if !ok || (!slices.Contains(reasons, value) && value != connector.ReasonOther) {
					t.Errorf("reason %q outside the declared vocabulary for signal %q", value, series["signal"])
				}
			default:
				if allowed, ok := closedValues[key]; ok {
					if !slices.Contains(allowed, value) {
						t.Errorf("label %s=%q outside its closed vocabulary %v", key, value, allowed)
					}
				} else if !slices.Contains(connector.LabelKeysAllowlist, key) {
					t.Errorf("label key %q outside the allowlist", key)
				}
			}
			if strings.Contains(value, "acme") || strings.Contains(value, "pull") ||
				strings.Contains(value, "http") || strings.Contains(value, "42") {
				t.Errorf("label %s carries free-form or identifying value %q", key, value)
			}
		}
	}
	catalogue := map[string]string{}
	for _, series := range gatheredConnectorSeries(t, f.reg) {
		if _, ok := series["capability"]; ok {
			catalogue[series["capability"]] = series["state"]
		}
	}
	for _, capability := range connector.Capabilities {
		state, ok := catalogue[capability]
		if !ok {
			t.Errorf("capability %q not declared in the catalogue", capability)
			continue
		}
		if !slices.Contains(connector.CapabilityStates, state) {
			t.Errorf("capability %q declared with state %q outside the closed states", capability, state)
		}
	}
}

// TestUntracedDeploymentEmitsWithoutCorrelationLabels is the
// no-correlation-label invariant (§11.4): a full discovery cycle with no
// tracing configured still emits the capability catalogue, diagnostics
// counters, and exclusion gauges, and no gathered series carries a
// correlation identifier label.
func TestUntracedDeploymentEmitsWithoutCorrelationLabels(t *testing.T) {
	f := newDiagnosticsFixture(t, func(c *HTTPClient) { c.WithPRFixExclusions(true) }, nextTaskIdle(map[string]any{"version": 1}))
	if _, err := f.client.DiscoverExcluding(context.Background(), uniqueExclusions(2)); err != nil {
		t.Fatal(err)
	}
	if _, ok := findSeries(t, f.reg, connector.MetricCapabilityInfo, map[string]string{
		"connector": defaultConnectorName, "capability": connector.CapabilityDiscovery, "state": connector.CapabilitySupported,
	}); !ok {
		t.Fatal("capability catalogue did not emit")
	}
	if got := diagnosticsCounter(t, f, connector.SignalAck, reasonAckOK); got != 1 {
		t.Fatalf("ack/ok counter = %v, want 1", got)
	}
	if got := diagnosticsCounter(t, f, connector.SignalAvailability, connector.AvailabilityAvailable); got != 1 {
		t.Fatalf("availability/available counter = %v, want 1", got)
	}
	conn := map[string]string{"connector": defaultConnectorName}
	for _, name := range []string{
		connector.MetricExclusionRequested, connector.MetricExclusionAccepted,
		connector.MetricExclusionOverflow, connector.MetricExclusionLimit,
	} {
		if _, ok := gaugeValue(t, f.reg, name, conn); !ok {
			t.Errorf("gauge %s did not emit", name)
		}
	}
	for _, series := range gatheredConnectorSeries(t, f.reg) {
		for key := range series {
			if slices.Contains(connector.CorrelationKeys, key) {
				t.Errorf("series carries correlation label %q: %v", key, series)
			}
		}
	}
}

// TestInvalidDescriptorFailsClosedWithDiagnostic pins the invalid-descriptor
// fatal path (§3.1): a descriptor failing the repo contract fails discovery,
// counts protocol-error/invalid-descriptor once, and no exclusion cycle is
// reported for the aborted attempt; a non-decodable ID stays silently
// skipped.
func TestInvalidDescriptorFailsClosedWithDiagnostic(t *testing.T) {
	var polls int
	f := newDiagnosticsFixture(t, func(c *HTTPClient) { c.WithPRFixExclusions(true) }, func(f *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
		polls++
		nextTaskIdle(map[string]any{"version": 1})(f, w, r)
	})
	items := []source.WorkItem{
		{ID: "not-a-work-id"}, // decode-skipped, silent
		{ID: followupWorkID(1)},
		{ID: EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "not a repo!", Number: 2}, PRFixItem: &PRFixItem{ID: "item-2", Generation: 1}})},
	}
	if _, err := f.client.DiscoverExcluding(context.Background(), items); err == nil {
		t.Fatal("invalid exclusion descriptor was accepted")
	}
	if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonInvalidDescriptor); got != 1 {
		t.Fatalf("protocol-error/invalid-descriptor counter = %v, want 1", got)
	}
	if polls != 0 {
		t.Fatalf("discovery requests = %d, want 0 (failed closed before send)", polls)
	}
	if got, ok := gaugeValue(t, f.reg, connector.MetricExclusionRequested, map[string]string{"connector": defaultConnectorName}); ok {
		t.Errorf("aborted cycle exported exclusion_requested = %v, want no cycle observation", got)
	}
}

// TestReportOutcomeDiagnostics covers the lifecycle-report boundary (§3):
// published on success, unsupported when the source rejects the outcome,
// retryable on other errors, and superseded reports counted under identity
// rather than a report reason.
func TestReportOutcomeDiagnostics(t *testing.T) {
	newReportFixture := func(status int, body string) *diagnosticsFixture {
		return newDiagnosticsFixture(t, nil, func(_ *diagnosticsFixture, w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		})
	}
	workID := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-1", Repo: "acme/widgets", Number: 42}})

	t.Run("published", func(t *testing.T) {
		f := newReportFixture(http.StatusOK, "{}")
		if err := f.client.Report(context.Background(), workID, source.Lifecycle{Result: source.ResultReady}); err != nil {
			t.Fatal(err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalReport, reasonReportPublished); got != 1 {
			t.Fatalf("report/published counter = %v, want 1", got)
		}
	})
	t.Run("unsupported", func(t *testing.T) {
		f := newReportFixture(http.StatusBadRequest, `{"error":"Invalid outcome. Must be one of: pr_opened, ..."}`)
		if err := f.client.Report(context.Background(), workID, source.Lifecycle{Result: source.ResultReady}); !errors.Is(err, source.ErrUnsupportedOutcome) {
			t.Fatalf("err = %v, want ErrUnsupportedOutcome", err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonUnsupportedTaskType); got != 1 {
			t.Fatalf("protocol-error/unsupported-task-type counter = %v, want 1", got)
		}
		if got := diagnosticsCounter(t, f, connector.SignalReport, reasonReportUnsupported); got != 1 {
			t.Fatalf("report/unsupported counter = %v, want 1", got)
		}
	})
	t.Run("retryable", func(t *testing.T) {
		f := newReportFixture(http.StatusInternalServerError, "boom")
		if err := f.client.Report(context.Background(), workID, source.Lifecycle{Result: source.ResultReady}); err == nil {
			t.Fatal("failed report unexpectedly succeeded")
		}
		if got := diagnosticsCounter(t, f, connector.SignalReport, reasonReportRetryable); got != 1 {
			t.Fatalf("report/retryable counter = %v, want 1", got)
		}
	})
	t.Run("superseded", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/pr-fix-queue/mark" {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"generation mismatch: expected 1"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		})
		item := followupWorkID(7)
		if err := f.client.Report(context.Background(), item, source.Lifecycle{Result: source.ResultBlocked, Error: "blocked"}); !errors.Is(err, source.ErrSuperseded) {
			t.Fatalf("err = %v, want ErrSuperseded", err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityGenerationAdvanced); got != 1 {
			t.Fatalf("identity/generation-advanced counter = %v, want 1", got)
		}
		if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentitySuperseded); got != 1 {
			t.Fatalf("identity/identity-superseded counter = %v, want 1", got)
		}
		if total := totalProtocolErrors(t, f); total != 0 {
			t.Fatalf("superseded exchanges counted %v protocol-error diagnostics, want 0", total)
		}
	})
	t.Run("no-exchange-results-fabricate-nothing", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, func(_ *diagnosticsFixture, _ http.ResponseWriter, r *http.Request) {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		})
		if err := f.client.Report(context.Background(), workID, source.Lifecycle{Result: source.ResultCapacityExhausted}); err != nil {
			t.Fatal(err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalReport, reasonReportPublished); got != -1 {
			t.Fatalf("unattempted report fabricated report/published = %v", got)
		}
		if got := diagnosticsCounter(t, f, connector.SignalReport, reasonReportUnsupported); got != -1 {
			t.Fatalf("unattempted report fabricated report/unsupported = %v", got)
		}
	})
}

// TestPreLaunchRevalidationDiagnostics covers the pre-launch boundary (§3.4):
// current on success, the actual staleness cause on a stale pull request, the
// released queue claim under identity, and a superseded generation counted as
// identity rather than protocol error.
func TestPreLaunchRevalidationDiagnostics(t *testing.T) {
	cases := []struct {
		name             string
		state            PullRequestState
		markStatus       int
		markBody         string
		wantRevalidation string
		wantClaimRelease bool
		wantErr          error
	}{
		{name: "current", state: PullRequestState{State: "open"}, wantRevalidation: reasonRevalidationCurrent},
		{name: "merged", state: PullRequestState{State: "", Merged: true}, markStatus: http.StatusOK, wantRevalidation: reasonRevalidationMerged, wantClaimRelease: true, wantErr: source.ErrStaleWork},
		{name: "closed-unmerged", state: PullRequestState{State: "closed"}, markStatus: http.StatusOK, wantRevalidation: reasonRevalidationClosedUnmerged, wantClaimRelease: true, wantErr: source.ErrStaleWork},
		{name: "superseded-mark", state: PullRequestState{State: "closed"}, markStatus: http.StatusConflict, markBody: `{"error":"generation mismatch: expected 2"}`, wantErr: source.ErrSuperseded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var marks int
			f := newDiagnosticsFixture(t, func(c *HTTPClient) {
				c.WithPullRequestStateChecker(PullRequestStateCheckerFunc(
					func(context.Context, string, int) (PullRequestState, error) {
						return tc.state, nil
					}))
			}, func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/pr-fix-queue/mark" {
					marks++
					w.WriteHeader(tc.markStatus)
					_, _ = w.Write([]byte(tc.markBody))
					return
				}
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			})
			err := f.client.PreLaunch(context.Background(), followupWorkID(7))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if got := diagnosticsCounter(t, f, connector.SignalRevalidation, tc.wantRevalidation); tc.wantRevalidation != "" && got != 1 {
				t.Fatalf("revalidation/%s counter = %v, want 1", tc.wantRevalidation, got)
			}
			got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityClaimReleased)
			if tc.wantClaimRelease && got != 1 {
				t.Fatalf("identity/claim-released counter = %v, want 1", got)
			}
			if !tc.wantClaimRelease && got != -1 {
				t.Fatalf("identity/claim-released counter = %v, want none", got)
			}
			if tc.markStatus == http.StatusConflict {
				if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityGenerationAdvanced); got != 1 {
					t.Fatalf("identity/generation-advanced counter = %v, want 1", got)
				}
				if total := totalProtocolErrors(t, f); total != 0 {
					t.Fatalf("superseded mark exchange counted %v protocol-error diagnostics, want 0", total)
				}
			}
			if tc.wantClaimRelease && marks == 0 {
				t.Fatalf("mark calls = %d, want the released queue claim", marks)
			}
		})
	}
}

// TestDefaultDiscoveryAvailabilityStates pins the availability observation
// (§3.5): unknown at construction, available after a successful poll,
// unreachable when the source cannot be reached at all.
func TestDefaultDiscoveryAvailabilityStates(t *testing.T) {
	t.Run("unknown-at-construction", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, nextTaskIdle(nil))
		if got := diagnosticsCounter(t, f, connector.SignalAvailability, connector.AvailabilityUnknown); got != 1 {
			t.Fatalf("availability/unknown counter at construction = %v, want 1", got)
		}
	})
	t.Run("available-after-success", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, nextTaskIdle(nil))
		if _, err := f.client.Discover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalAvailability, connector.AvailabilityAvailable); got != 1 {
			t.Fatalf("availability/available counter = %v, want 1", got)
		}
	})
	t.Run("unreachable-on-transport-failure", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, func(_ *diagnosticsFixture, _ http.ResponseWriter, _ *http.Request) {})
		f.server.Close()
		if _, err := f.client.Discover(context.Background()); err == nil {
			t.Fatal("unreachable source unexpectedly succeeded")
		}
		if got := diagnosticsCounter(t, f, connector.SignalAvailability, connector.AvailabilityUnreachable); got != 1 {
			t.Fatalf("availability/unreachable counter = %v, want 1", got)
		}
		if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonTransportError); got != 1 {
			t.Fatalf("protocol-error/transport-error counter = %v, want 1", got)
		}
	})
}

// TestConfiguredAdaptersKeepIndependentSeries is the connector-identity
// regression (§2): two configured Dispatch adapters sharing one emitter each
// emit under their own bounded connector label, so neither adapter's
// diagnostics, exclusion gauges, capability catalogue, or availability state
// collide with — or overwrite — the other's.
func TestConfiguredAdaptersKeepIndependentSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()

	for _, name := range []string{"dispatch:cloud", "dispatch:local"} {
		client, err := NewClient(server.URL, strings.TrimPrefix(name, "dispatch:"), "token", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		New(client, WithConnectorDiagnostics(em), WithConnectorName(name))
		if _, err := client.Discover(context.Background()); err == nil {
			t.Fatal("failing poll unexpectedly succeeded")
		}
	}

	for _, name := range []string{"dispatch:cloud", "dispatch:local"} {
		if got, ok := counterValue(t, reg, map[string]string{
			"connector": name, "signal": connector.SignalProtocolError, "reason": reasonHTTP5xx,
		}); !ok || got != 1 {
			t.Fatalf("%s protocol-error/http-5xx counter = %v, ok=%v; want 1 under its own series", name, got, ok)
		}
		if got, ok := counterValue(t, reg, map[string]string{
			"connector": name, "signal": connector.SignalAvailability, "reason": connector.AvailabilityUnknown,
		}); !ok || got != 1 {
			t.Fatalf("%s availability/unknown counter = %v, ok=%v; want 1 under its own series", name, got, ok)
		}
		if _, ok := gaugeValue(t, reg, connector.MetricExclusionLimit, map[string]string{"connector": name}); !ok {
			t.Fatalf("%s exclusion limit gauge missing", name)
		}
		if _, ok := gaugeValue(t, reg, connector.MetricExclusionPressure, map[string]string{"connector": name}); ok {
			t.Fatalf("%s exclusion pressure gauge exists without a cycle; series leaked from another connector", name)
		}
		if _, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
			"connector": name, "capability": connector.CapabilityDiscovery, "state": connector.CapabilitySupported,
		}); !ok {
			t.Fatalf("%s capability catalogue missing", name)
		}
	}
	if _, ok := counterValue(t, reg, map[string]string{
		"connector": defaultConnectorName, "signal": connector.SignalProtocolError, "reason": reasonHTTP5xx,
	}); ok {
		t.Fatal("configured adapters leaked diagnostics onto the legacy bare dispatch series")
	}
	if _, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
		"connector": defaultConnectorName, "capability": connector.CapabilityDiscovery,
	}); ok {
		t.Fatal("configured adapters leaked capability series onto the legacy bare dispatch series")
	}
}

// TestLegacyAndShorthandAdaptersShareOneConnectorIdentity pins the one
// connector identity Dispatch adapters share on purpose. The manager keeps a
// legacy bare adapter for runs retained from before source-agent routing, and
// in the single-binding shorthand the lane binding carries no source agent —
// so both adapters talk to the same Dispatch agent and both report under the
// bare "dispatch" identity, on one shared series. That is a deliberate
// single-agent identity, not a collision between independent connectors: a
// configured binding keeps its own identity and never lands on that series.
func TestLegacyAndShorthandAdaptersShareOneConnectorIdentity(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()

	// The legacy bare adapter and the shorthand lane binding: same agent,
	// same bare connector identity, exactly as cmd/main.go builds them when
	// the binding carries no source agent.
	legacyClient, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	New(legacyClient, WithConnectorDiagnostics(em))
	shorthandClient, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	New(shorthandClient, WithConnectorDiagnostics(em))

	for _, client := range []*HTTPClient{legacyClient, shorthandClient} {
		if _, err := client.Discover(context.Background()); err == nil {
			t.Fatal("failing poll unexpectedly succeeded")
		}
	}

	// Both adapters' diagnostics land on the one shared bare series.
	if got, ok := counterValue(t, reg, map[string]string{
		"connector": defaultConnectorName, "signal": connector.SignalProtocolError, "reason": reasonHTTP5xx,
	}); !ok || got != 2 {
		t.Fatalf("bare dispatch protocol-error counter = %v, ok=%v; want 2 on the one shared series", got, ok)
	}
	if _, ok := gaugeValue(t, reg, connector.MetricExclusionLimit, map[string]string{"connector": defaultConnectorName}); !ok {
		t.Fatal("bare dispatch exclusion limit gauge missing")
	}
	if _, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
		"connector": defaultConnectorName, "capability": connector.CapabilityDiscovery, "state": connector.CapabilitySupported,
	}); !ok {
		t.Fatal("bare dispatch capability catalogue missing")
	}

	// A configured binding keeps its own identity and stays off the shared
	// series, so the sharing above stays bounded to one Dispatch agent.
	configured, err := NewClient(server.URL, "cloud-worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	New(configured, WithConnectorDiagnostics(em), WithConnectorName("dispatch:cloud"))
	if _, err := configured.Discover(context.Background()); err == nil {
		t.Fatal("failing poll unexpectedly succeeded")
	}
	if got, ok := counterValue(t, reg, map[string]string{
		"connector": defaultConnectorName, "signal": connector.SignalProtocolError, "reason": reasonHTTP5xx,
	}); !ok || got != 2 {
		t.Fatalf("bare dispatch protocol-error counter = %v, ok=%v; want 2, the configured adapter must not land on it", got, ok)
	}
	if got, ok := counterValue(t, reg, map[string]string{
		"connector": "dispatch:cloud", "signal": connector.SignalProtocolError, "reason": reasonHTTP5xx,
	}); !ok || got != 1 {
		t.Fatalf("dispatch:cloud protocol-error counter = %v, ok=%v; want 1 under its own series", got, ok)
	}
}

// TestWithConnectorNameRejectsUnboundedIdentity pins the loud-failure
// contract for configured connector identities: an empty, over-long, or
// out-of-charset label value fails at construction instead of silently
// merging independent metric series.
func TestWithConnectorNameRejectsUnboundedIdentity(t *testing.T) {
	for _, name := range []string{"", ":", strings.Repeat("a", MaxConnectorNameLength+1), "has space", "bad/slash"} {
		client, err := NewClient("http://localhost", "worker", "token", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("connector name %q unexpectedly accepted", name)
				}
			}()
			New(client, WithConnectorName(name))
		}()
	}
}
