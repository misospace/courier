package manual

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

func findSeries(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
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
				switch {
				case m.GetCounter() != nil:
					return m.GetCounter().GetValue(), true
				case m.GetGauge() != nil:
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func diagnosticsCounter(t *testing.T, reg *prometheus.Registry, signal, reason string) (float64, bool) {
	t.Helper()
	return findSeries(t, reg, connector.MetricDiagnosticsTotal, map[string]string{
		"connector": connectorName, "signal": signal, "reason": reason,
	})
}

// TestManualCapabilityCatalogue pins the manual adapter's declared
// capabilities (§5, §8): only discovery and availability are supported and
// every other capability is explicitly unsupported, so a dashboard renders
// the omission instead of a fabricated panel.
func TestManualCapabilityCatalogue(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	_ = New(em)

	states := map[string]string{
		connector.CapabilityDiscovery:             connector.CapabilitySupported,
		connector.CapabilityDiscoveryExclusions:   connector.CapabilityUnsupported,
		connector.CapabilityLifecycleReport:       connector.CapabilityUnsupported,
		connector.CapabilityPreLaunchRevalidation: connector.CapabilityUnsupported,
		connector.CapabilityCIParkLifecycle:       connector.CapabilityUnsupported,
		connector.CapabilityWorkIdentity:          connector.CapabilityUnsupported,
		connector.CapabilityAvailability:          connector.CapabilitySupported,
	}
	for capability, state := range states {
		got, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
			"connector": connectorName, "capability": capability, "state": state,
		})
		if !ok || got != 1 {
			t.Errorf("capability %q state %q = %v, ok=%v; want 1", capability, state, got, ok)
		}
	}
}

// TestManualAvailabilityObservations checks the availability emissions: the
// zero-value adapter stays diagnostic-free, a successful Discover counts
// available, and a failing one counts unreachable.
func TestManualAvailabilityObservations(t *testing.T) {
	t.Run("zero-value-stays-silent", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		quiet := Adapter{}
		if _, err := quiet.Discover(context.Background()); err != nil {
			t.Fatal(err)
		}
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			if f.GetName() == connector.MetricDiagnosticsTotal && len(f.GetMetric()) != 0 {
				t.Fatalf("unregistered adapter emitted diagnostics: %v", f.GetMetric())
			}
		}
	})
	t.Run("available-on-success", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		em := connector.NewEmitter(reg, nil)
		a := New(em)
		if _, err := a.Discover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got, ok := diagnosticsCounter(t, reg, connector.SignalAvailability, reasonAvailable); !ok || got != 1 {
			t.Fatalf("availability/available counter = %v, ok=%v; want 1", got, ok)
		}
	})
	t.Run("unreachable-on-failure", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		em := connector.NewEmitter(reg, nil)
		a := newWithDiscover(em, func(context.Context) ([]source.WorkItem, error) {
			return nil, errors.New("source unreachable")
		})
		if _, err := a.Discover(context.Background()); err == nil {
			t.Fatal("failing discovery unexpectedly succeeded")
		}
		if got, ok := diagnosticsCounter(t, reg, connector.SignalAvailability, reasonUnreachable); !ok || got != 1 {
			t.Fatalf("availability/unreachable counter = %v, ok=%v; want 1", got, ok)
		}
	})
}

// TestManualAttemptRequeue pins the manual adapter's implementation of the
// optional fenced requeue capability (source.AttemptRequeuer, #609): a
// validated no-op that mints the fresh attempt identity in the manual
// namespace — "<WorkItemID>/retry/<requestID>" — and directs the core to
// materialize the successor, because no external source state exists for
// discovery to observe.
func TestManualAttemptRequeue(t *testing.T) {
	t.Run("mints-successor-identity", func(t *testing.T) {
		a := Adapter{}
		result, err := a.RequeueAttempt(context.Background(),
			source.WorkItem{ID: "wid"},
			source.AttemptRequeueRequest{IdempotencyKey: "key-1", Reason: "req-1"})
		if err != nil {
			t.Fatalf("RequeueAttempt: %v", err)
		}
		if result.AttemptID != "wid/retry/req-1" {
			t.Errorf("AttemptID = %q; want %q", result.AttemptID, "wid/retry/req-1")
		}
		if !result.ControllerMaterializesSuccessor {
			t.Error("ControllerMaterializesSuccessor = false; want true")
		}
	})
	t.Run("identical-inputs-replay-identical-result", func(t *testing.T) {
		a := Adapter{}
		item := source.WorkItem{ID: "wid"}
		req := source.AttemptRequeueRequest{IdempotencyKey: "key-1", Reason: "req-1"}
		first, err := a.RequeueAttempt(context.Background(), item, req)
		if err != nil {
			t.Fatalf("first RequeueAttempt: %v", err)
		}
		second, err := a.RequeueAttempt(context.Background(), item, req)
		if err != nil {
			t.Fatalf("second RequeueAttempt: %v", err)
		}
		if first != second {
			t.Errorf("replay = %+v; want %+v", second, first)
		}
	})
	t.Run("non-empty-fence-refuses-stale-generation", func(t *testing.T) {
		a := Adapter{}
		_, err := a.RequeueAttempt(context.Background(),
			source.WorkItem{ID: "wid"},
			source.AttemptRequeueRequest{IdempotencyKey: "key-1", Reason: "req-1", FenceToken: "claimed-elsewhere"})
		var refusal *source.RetryRefusalError
		if !errors.As(err, &refusal) {
			t.Fatalf("error = %v; want *source.RetryRefusalError", err)
		}
		if refusal.Reason != retryReasonStaleGeneration {
			t.Errorf("refusal.Reason = %q; want %q", refusal.Reason, retryReasonStaleGeneration)
		}
		if refusal.Moot {
			t.Error("refusal.Moot = true; want false")
		}
	})
	t.Run("blank-item-id-fails-closed", func(t *testing.T) {
		a := Adapter{}
		result, err := a.RequeueAttempt(context.Background(),
			source.WorkItem{ID: "  "},
			source.AttemptRequeueRequest{IdempotencyKey: "key-1", Reason: "req-1"})
		if err == nil {
			t.Fatal("RequeueAttempt succeeded; want error")
		}
		var refusal *source.RetryRefusalError
		if errors.As(err, &refusal) {
			t.Fatalf("error = %v; want plain error, not a refusal", err)
		}
		if result != (source.AttemptRequeueResult{}) {
			t.Errorf("result = %+v; want zero", result)
		}
	})
	t.Run("blank-idempotency-key-fails-closed", func(t *testing.T) {
		a := Adapter{}
		result, err := a.RequeueAttempt(context.Background(),
			source.WorkItem{ID: "wid"},
			source.AttemptRequeueRequest{IdempotencyKey: "", Reason: "req-1"})
		if err == nil {
			t.Fatal("RequeueAttempt succeeded; want error")
		}
		var refusal *source.RetryRefusalError
		if errors.As(err, &refusal) {
			t.Fatalf("error = %v; want plain error, not a refusal", err)
		}
		if result != (source.AttemptRequeueResult{}) {
			t.Errorf("result = %+v; want zero", result)
		}
	})
	t.Run("blank-reason-fails-closed", func(t *testing.T) {
		a := Adapter{}
		result, err := a.RequeueAttempt(context.Background(),
			source.WorkItem{ID: "wid"},
			source.AttemptRequeueRequest{IdempotencyKey: "key-1", Reason: " "})
		if err == nil {
			t.Fatal("RequeueAttempt succeeded; want error")
		}
		var refusal *source.RetryRefusalError
		if errors.As(err, &refusal) {
			t.Fatalf("error = %v; want plain error, not a refusal", err)
		}
		if result != (source.AttemptRequeueResult{}) {
			t.Errorf("result = %+v; want zero", result)
		}
	})
}
