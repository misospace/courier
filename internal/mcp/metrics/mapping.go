package metrics

import (
	"fmt"

	dto "github.com/prometheus/client_model/go"
)

// Backend mapping names accepted by For.
const (
	BackendAuto = "auto"
	BackendVLLM = "vllm"
)

// Mapping normalizes one model-server family's Prometheus metrics into
// Signals. Map sets the gauges the backend actually exposes and reports
// whether it recognized the endpoint at all; a mapping that recognizes
// nothing must say so rather than let the caller invent numbers.
type Mapping interface {
	Name() string
	Map(families []*dto.MetricFamily) (Signals, bool)
}

// For returns the mapping for a configured backend name. "auto" (or empty)
// sniffs the exposed metric names; a named backend forces the mapping so a
// deployment can pin one even where sniffing would be ambiguous.
func For(backend string) (Mapping, error) {
	switch backend {
	case "", BackendAuto:
		return autoMapping{}, nil
	case BackendVLLM:
		return vllmMapping{}, nil
	default:
		return nil, fmt.Errorf("unknown backend mapping %q (supported: %s, %s)", backend, BackendAuto, BackendVLLM)
	}
}

// autoMapping infers the backend from the exposed metric names. Sniffing is
// safe because the namespaces are disjoint (vllm:, sglang:, ...); anything
// unrecognized stays unrecognized — auto never guesses a mapping.
type autoMapping struct{}

func (autoMapping) Name() string { return BackendAuto }

func (autoMapping) Map(families []*dto.MetricFamily) (Signals, bool) {
	if !vllmExposed(families) {
		return Signals{}, false
	}
	return vllmMapping{}.Map(families)
}

// gaugeValues collects the worst-case sample of each gauge, keyed by family
// name. The max is order-independent (parsers may deliver families in any
// order) and conservative: with several labeled series, any one queuing or
// saturating is the load signal that matters. Counters and histograms are
// not load gauges and are skipped.
func gaugeValues(families []*dto.MetricFamily) map[string]*float64 {
	values := make(map[string]*float64)
	for _, family := range families {
		if family.GetType() != dto.MetricType_GAUGE {
			continue
		}
		name := family.GetName()
		for _, metric := range family.GetMetric() {
			if metric.GetGauge() == nil {
				continue
			}
			value := metric.GetGauge().GetValue()
			if existing, ok := values[name]; ok && *existing >= value {
				continue
			}
			values[name] = &value
		}
	}
	return values
}
