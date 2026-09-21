// Package metrics exposes model-server load as one read-only MCP tool. It
// scrapes a configured Prometheus /metrics endpoint, maps the backend's
// gauges into a generic load shape, and degrades to an honest "unavailable"
// result when the endpoint is down or unfamiliar. Load is context for the
// coordinator, never a gate: the tool informs fan-out decisions, it makes
// none.
package metrics

// Load is the backend-neutral result of one scrape.
//
// Gauges the backend does not expose stay unset rather than zero: unknown
// and zero mean different things, and the tool must not fabricate either.
type Load struct {
	// Available reports whether the endpoint answered with metrics that map
	// to a known backend.
	Available bool `json:"available"`
	// Detail explains unavailability in one short line; empty when available.
	Detail string `json:"detail,omitempty"`
	// Running is the number of requests currently executing, when known.
	Running *float64 `json:"running,omitempty"`
	// Waiting is the number of requests queued for a slot, when known. This
	// is the saturation signal: a queue forming means the backend cannot
	// keep up with the offered load.
	Waiting *float64 `json:"waiting,omitempty"`
	// Backpressured reports whether meaningful backpressure exists. It is
	// derived from Waiting alone — running requests are not evidence of
	// overload, since one long generation can hold the only slot on an
	// otherwise healthy backend. Unset when Waiting is unknown.
	Backpressured *bool `json:"backpressured,omitempty"`
	// KVCacheUsage is the backend's cache utilization in [0,1], when
	// reported. Secondary context, not a saturation signal.
	KVCacheUsage *float64 `json:"kvCacheUsage,omitempty"`
}

// Signals are the load gauges a backend Mapping recognized in a scrape.
type Signals struct {
	Running      *float64
	Waiting      *float64
	KVCacheUsage *float64
}

// unavailable is the load shape for a scrape that yielded nothing usable.
func unavailable(detail string) Load {
	return Load{Available: false, Detail: detail}
}

// normalize derives the generic load shape from backend signals.
func normalize(signals Signals) Load {
	return Load{
		Available:     true,
		Running:       signals.Running,
		Waiting:       signals.Waiting,
		Backpressured: backpressured(signals.Waiting),
		KVCacheUsage:  signals.KVCacheUsage,
	}
}

func backpressured(waiting *float64) *bool {
	if waiting == nil {
		return nil
	}
	backpressured := *waiting > 0
	return &backpressured
}
