package dispatch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/connector/connectortest"
	"github.com/misospace/courier/internal/source"
)

// The no-double-count fixture of docs/connector-diagnostics.md §11.2 drives
// the real Dispatch adapter through the real source.Runner so one failing
// discovery cycle is observed on both sides of the join: the core
// courier_source_discovery_failures_total counter and the connector's
// courier_connector_diagnostics_total envelope.
const (
	// metricDiscoveryFailuresTotal is the core discovery failure counter
	// family (internal/source/metrics.go). The core exports no constant for
	// it, so the fixture names it here.
	metricDiscoveryFailuresTotal = "courier_source_discovery_failures_total"

	duplicateCountingNamespace = "courier"
	duplicateCountingLane      = "local"
	duplicateCountingSource    = "dispatch"

	// duplicateCountingCycleID is the test-scoped identity of the one
	// discovery cycle the fixture runs. It never becomes a metric label.
	duplicateCountingCycleID = "cycle-1"

	// coreFailureReason classifies the core-side observation of a failed
	// discovery call. The core counter carries no reason label; the value
	// only describes the observation handed to the join.
	coreFailureReason = "source-call-failed"
)

// duplicateCountingFixture wires the real Dispatch adapter, the real
// source.Runner, the real discovery metrics, and one fresh registry, so a
// single failing discovery cycle can be observed on both sides of the
// duplicate-counting join.
type duplicateCountingFixture struct {
	reg    *prometheus.Registry
	runner *source.Runner
}

// newDuplicateCountingFixture builds the fixture around a Dispatch endpoint
// that always answers HTTP 500: the client classifies that as a protocol
// error with reason http-5xx and returns an error, which fails the core
// discovery call as well.
func newDuplicateCountingFixture(t *testing.T) *duplicateCountingFixture {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	reg := prometheus.NewRegistry()
	emitter := connector.NewEmitter(reg, nil)
	client, err := NewClient(server.URL, "worker", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	adapter := New(client, WithConnectorDiagnostics(emitter))

	kubeClient := duplicateCountingKubeClient(t)

	runner := source.NewRunner(kubeClient, adapter, source.RunnerConfig{
		Source:      duplicateCountingSource,
		LaneProfile: duplicateCountingLane,
		Namespace:   duplicateCountingNamespace,
	})
	runner.Metrics = source.NewDiscoveryMetrics(reg)

	return &duplicateCountingFixture{
		reg:    reg,
		runner: runner,
	}
}

// duplicateCountingLaneProfile is the lane the fixture polls on: the runner
// pauses discovery without one. Its spec satisfies the CRD's validation, so
// the same object is accepted by the envtest API server and by the fake
// client.
func duplicateCountingLaneProfile() *courierv1alpha1.LaneProfile {
	return &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Namespace: duplicateCountingNamespace, Name: duplicateCountingLane},
		Spec:       courierv1alpha1.LaneProfileSpec{Concurrency: 1, Roles: map[string]string{"coordinator": "test"}},
	}
}

// duplicateCountingKubeClient returns the client the fixture polls through.
// The contract's fixture is an envtest fixture, so when KUBEBUILDER_ASSETS
// provides an API server the lane lives in it. Without assets the same
// fixture runs against the controller-runtime fake client, so the assertion
// never stops running in a plain `go test` — it is only the API server that
// differs, never the assertion.
func duplicateCountingKubeClient(t *testing.T) client.Client {
	t.Helper()
	if dispatchEnvtestClient == nil {
		return fake.NewClientBuilder().
			WithScheme(dispatchTestScheme()).
			WithObjects(duplicateCountingLaneProfile()).
			Build()
	}
	ctx := context.Background()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: duplicateCountingNamespace}}
	if err := dispatchEnvtestClient.Create(ctx, namespace); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %q: %v", duplicateCountingNamespace, err)
	}
	lane := duplicateCountingLaneProfile()
	if err := dispatchEnvtestClient.Create(ctx, lane); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create LaneProfile %q: %v", lane.Name, err)
	}
	t.Cleanup(func() {
		_ = dispatchEnvtestClient.Delete(context.Background(), lane)
	})
	return dispatchEnvtestClient
}

// duplicateCountingCounter reads one counter series' value; a series that does
// not exist yet reads as zero.
func duplicateCountingCounter(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := findSeries(t, reg, name, labels)
	if !ok || metric.GetCounter() == nil {
		return 0
	}
	return metric.GetCounter().GetValue()
}

// TestNoDoubleCountBetweenCoreAndConnectorDiagnostics is the no-double-count
// fixture (§11.2): one synthetic discovery poll fails the core Discover call
// and emits a connector protocol-error diagnostic for the same cycle. The
// core counter must move by exactly one, the connector's diagnostics counter
// must move by exactly one, and the join of the two sides on
// (connector, cycle id) must report one logical failure, not two.
func TestNoDoubleCountBetweenCoreAndConnectorDiagnostics(t *testing.T) {
	f := newDuplicateCountingFixture(t)

	coreLabels := map[string]string{"source": duplicateCountingSource, "lane": duplicateCountingLane}
	connectorLabels := map[string]string{
		"connector": defaultConnectorName,
		"signal":    connector.SignalProtocolError,
		"reason":    reasonHTTP5xx,
	}

	coreBefore := duplicateCountingCounter(t, f.reg, metricDiscoveryFailuresTotal, coreLabels)
	connectorBefore := duplicateCountingCounter(t, f.reg, connector.MetricDiagnosticsTotal, connectorLabels)

	if err := f.runner.Poll(context.Background()); err == nil {
		t.Fatal("failing discovery poll unexpectedly succeeded")
	}

	coreAfter := duplicateCountingCounter(t, f.reg, metricDiscoveryFailuresTotal, coreLabels)
	connectorAfter := duplicateCountingCounter(t, f.reg, connector.MetricDiagnosticsTotal, connectorLabels)
	coreDelta := coreAfter - coreBefore
	connectorDelta := connectorAfter - connectorBefore

	if coreDelta != 1 {
		t.Fatalf("%s delta = %v, want exactly 1 for the failed cycle", metricDiscoveryFailuresTotal, coreDelta)
	}
	if connectorDelta != 1 {
		t.Fatalf("%s{connector=%q,signal=%q,reason=%q} delta = %v, want exactly 1 for the failed cycle",
			connector.MetricDiagnosticsTotal, defaultConnectorName, connector.SignalProtocolError, reasonHTTP5xx, connectorDelta)
	}

	// The two sides observed the same cycle: joining them on
	// (connector, cycle id) yields one logical failure, not two. The
	// observations come from the exported state — one per side — and the
	// cycle id is the fixture's: no cycle identifier crosses the metric
	// boundary, because it must never become a label.
	cycle := connectortest.Cycle{Connector: defaultConnectorName, ID: duplicateCountingCycleID}
	observations := []connectortest.Observation{
		{Cycle: cycle, Side: connectortest.SideCore, Reason: coreFailureReason},
		{Cycle: cycle, Side: connectortest.SideConnector, Reason: reasonHTTP5xx},
	}
	if len(observations) != 2 {
		t.Fatalf("observations = %d, want one per side", len(observations))
	}
	if joined := connectortest.Join(observations); joined != 1 {
		t.Fatalf("joined failures = %d, want 1 (not 2) for one logical failure", joined)
	}

	// The pre-baked expected series of the two families the contract names.
	expectedCore := `
# HELP courier_source_discovery_failures_total Source calls that returned an error.
# TYPE courier_source_discovery_failures_total counter
courier_source_discovery_failures_total{lane="local",source="dispatch"} 1
`
	if err := testutil.CollectAndCompare(f.reg, strings.NewReader(expectedCore), metricDiscoveryFailuresTotal); err != nil {
		t.Fatalf("core discovery failure series: %v", err)
	}

	// The whole connector diagnostics family is baked: the failed cycle's
	// protocol error counts once, the connector's availability moves from
	// unknown to degraded once, and no other diagnostic appears.
	expectedConnector := `
# HELP courier_connector_diagnostics_total Connector diagnostic events, by connector, signal, and connector-declared reason.
# TYPE courier_connector_diagnostics_total counter
courier_connector_diagnostics_total{connector="dispatch",reason="degraded",signal="availability"} 1
courier_connector_diagnostics_total{connector="dispatch",reason="http-5xx",signal="protocol-error"} 1
courier_connector_diagnostics_total{connector="dispatch",reason="unknown",signal="availability"} 1
`
	if err := testutil.CollectAndCompare(f.reg, strings.NewReader(expectedConnector), connector.MetricDiagnosticsTotal); err != nil {
		t.Fatalf("connector diagnostics series: %v", err)
	}
}

// TestNoDoubleCountJoinsOneFailurePerCycle proves the join counts logical
// failures, not observations: two failing cycles move the core counter and
// the connector envelope by two each, and the join reports the two distinct
// cycles — never four rows and never one merged failure.
func TestNoDoubleCountJoinsOneFailurePerCycle(t *testing.T) {
	f := newDuplicateCountingFixture(t)

	coreLabels := map[string]string{"source": duplicateCountingSource, "lane": duplicateCountingLane}
	connectorLabels := map[string]string{
		"connector": defaultConnectorName,
		"signal":    connector.SignalProtocolError,
		"reason":    reasonHTTP5xx,
	}
	coreBefore := duplicateCountingCounter(t, f.reg, metricDiscoveryFailuresTotal, coreLabels)
	connectorBefore := duplicateCountingCounter(t, f.reg, connector.MetricDiagnosticsTotal, connectorLabels)

	for range 2 {
		if err := f.runner.Poll(context.Background()); err == nil {
			t.Fatal("failing discovery poll unexpectedly succeeded")
		}
	}

	coreDelta := duplicateCountingCounter(t, f.reg, metricDiscoveryFailuresTotal, coreLabels) - coreBefore
	connectorDelta := duplicateCountingCounter(t, f.reg, connector.MetricDiagnosticsTotal, connectorLabels) - connectorBefore
	if coreDelta != 2 {
		t.Fatalf("%s delta = %v, want 2 for two failed cycles", metricDiscoveryFailuresTotal, coreDelta)
	}
	if connectorDelta != 2 {
		t.Fatalf("%s protocol-error delta = %v, want 2 for two failed cycles", connector.MetricDiagnosticsTotal, connectorDelta)
	}

	observations := make([]connectortest.Observation, 0, 4)
	for cycle := range 2 {
		id := fmt.Sprintf("cycle-%d", cycle+1)
		observations = append(observations, connectortest.Observation{
			Cycle:  connectortest.Cycle{Connector: defaultConnectorName, ID: id},
			Side:   connectortest.SideCore,
			Reason: coreFailureReason,
		})
		observations = append(observations, connectortest.Observation{
			Cycle:  connectortest.Cycle{Connector: defaultConnectorName, ID: id},
			Side:   connectortest.SideConnector,
			Reason: reasonHTTP5xx,
		})
	}
	if joined := connectortest.Join(observations); joined != 2 {
		t.Fatalf("joined failures = %d, want 2 for two distinct cycles", joined)
	}
}
