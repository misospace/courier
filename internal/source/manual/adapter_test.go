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
