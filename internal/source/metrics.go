package source

import (
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// DiscoverySkipReasons is the closed vocabulary of reasons the runner drops an
// offered item during validation. Tests reference this slice as the single
// source of truth.
var DiscoverySkipReasons = []string{"missing-id", "unsupported-mode", "invalid-ref"}

// DiscoveryPauseReasons is the closed vocabulary of reasons a poll is skipped
// before the source is called. capacity-cooldown was added by #327 after the
// design text was written; the vocabulary stays closed — new reasons require
// updating this slice, not free-form strings at emission sites.
var DiscoveryPauseReasons = []string{"lane-waiting", "lane-suspended", "lane-at-capacity", "capacity-cooldown"}

// pausedDesc describes the paused-state gauge. The metric name, help text,
// and label set are the ones the gauge family has always published.
var pausedDesc = prometheus.NewDesc(
	"courier_source_discovery_paused",
	"1 while the binding is currently paused before calling the source, 0 otherwise; exactly one reason is 1 per paused binding.",
	[]string{"source", "lane", "reason"},
	nil,
)

// DiscoveryMetrics is the discovery and lane-capacity signal layer for one
// process (§3.2 of docs/observability-signals-and-health.md). Every emission
// method is nil-safe: a nil *DiscoveryMetrics is a no-op, so a Runner built
// without Metrics emits nothing and keeps its old behaviour. Only
// NewDiscoveryMetrics builds a usable value — the zero-value struct is not
// usable and panics on emission.
type DiscoveryMetrics struct {
	attempts     *prometheus.CounterVec
	successes    *prometheus.CounterVec
	failures     *prometheus.CounterVec
	offered      *prometheus.CounterVec
	skipped      *prometheus.CounterVec
	materialized *prometheus.CounterVec
	lastAttempt  *prometheus.GaugeVec
	lastSuccess  *prometheus.GaugeVec
	pausedTotal  *prometheus.CounterVec
	paused       *pausedCollector
}

// NewDiscoveryMetrics builds and registers the discovery metrics on reg.
func NewDiscoveryMetrics(reg prometheus.Registerer) *DiscoveryMetrics {
	m := &DiscoveryMetrics{
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_discovery_attempts_total",
			Help: "Polls that actually called the source; polls skipped for lane-waiting, suspension, cooldown, or capacity are not attempts.",
		}, []string{"source", "lane"}),
		successes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_discovery_successes_total",
			Help: "Source calls that returned without error, including zero items, so an idle queue is observable as healthy.",
		}, []string{"source", "lane"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_discovery_failures_total",
			Help: "Source calls that returned an error.",
		}, []string{"source", "lane"}),
		offered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_items_offered_total",
			Help: "Items returned by discovery before core validation and dedupe.",
		}, []string{"source", "lane"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_discovery_skipped_total",
			Help: "Items dropped by runner validation, by reason.",
		}, []string{"source", "lane", "reason"}),
		materialized: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_items_materialized_total",
			Help: "Items that became CoderRuns; dedupe and exclusions may make this smaller than items offered.",
		}, []string{"source", "lane"}),
		lastAttempt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "courier_source_last_attempt_timestamp_seconds",
			Help: "Unix time of the last poll that called the source.",
		}, []string{"source", "lane"}),
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "courier_source_last_success_timestamp_seconds",
			Help: "Unix time of the last successful discovery; absent until the first success — absence is starting, never idle.",
		}, []string{"source", "lane"}),
		pausedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_discovery_paused_total",
			Help: "Polls skipped before the source was called, by reason.",
		}, []string{"source", "lane", "reason"}),
		paused: newPausedCollector(),
	}
	reg.MustRegister(
		m.attempts, m.successes, m.failures, m.offered, m.skipped,
		m.materialized, m.lastAttempt, m.lastSuccess, m.pausedTotal, m.paused,
	)
	return m
}

// IncAttempts records a poll that called the source.
func (m *DiscoveryMetrics) IncAttempts(source, lane string) {
	if m == nil {
		return
	}
	m.attempts.WithLabelValues(source, lane).Inc()
}

// IncSuccesses records a source call that returned without error.
func (m *DiscoveryMetrics) IncSuccesses(source, lane string) {
	if m == nil {
		return
	}
	m.successes.WithLabelValues(source, lane).Inc()
}

// IncFailures records a source call that returned an error.
func (m *DiscoveryMetrics) IncFailures(source, lane string) {
	if m == nil {
		return
	}
	m.failures.WithLabelValues(source, lane).Inc()
}

// AddOffered records items returned by discovery before validation.
func (m *DiscoveryMetrics) AddOffered(source, lane string, items int) {
	if m == nil {
		return
	}
	m.offered.WithLabelValues(source, lane).Add(float64(items))
}

// IncSkipped records one offered item dropped by validation.
func (m *DiscoveryMetrics) IncSkipped(source, lane, reason string) {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues(source, lane, reason).Inc()
}

// IncMaterialized records one item that became a CoderRun.
func (m *DiscoveryMetrics) IncMaterialized(source, lane string) {
	if m == nil {
		return
	}
	m.materialized.WithLabelValues(source, lane).Inc()
}

// SetLastAttempt records the unix time of the last poll that called the source.
func (m *DiscoveryMetrics) SetLastAttempt(source, lane string, unixSeconds int64) {
	if m == nil {
		return
	}
	m.lastAttempt.WithLabelValues(source, lane).Set(float64(unixSeconds))
}

// SetLastSuccess records the unix time of the last successful discovery. The
// series is created by this call, so it stays absent until the first success.
func (m *DiscoveryMetrics) SetLastSuccess(source, lane string, unixSeconds int64) {
	if m == nil {
		return
	}
	m.lastSuccess.WithLabelValues(source, lane).Set(float64(unixSeconds))
}

// SetPaused marks the binding paused for exactly one reason: that reason's
// gauge series is 1 and every other reason's is 0. Several runners may share
// one (source, lane) pair, so the gauge reflects the most recent poll's
// evaluation of that pair's lane-wide pause state; every runner re-evaluates
// the same lane-scoped gates each poll, so the value self-corrects within one
// poll interval. The reason is stored in one map write, so a reason change is
// never observable half applied: a scrape sees either the previous reason or
// the new one, never both at 1.
func (m *DiscoveryMetrics) SetPaused(source, lane, reason string) {
	if m == nil {
		return
	}
	m.pausedTotal.WithLabelValues(source, lane, reason).Inc()
	m.paused.set(source, lane, reason)
}

// ClearPaused marks the binding not paused: every reason's gauge series is 0.
func (m *DiscoveryMetrics) ClearPaused(source, lane string) {
	if m == nil {
		return
	}
	m.paused.set(source, lane, "")
}

// bindingKey identifies one binding — the (source, lane) pair a pause state
// belongs to.
type bindingKey struct {
	source string
	lane   string
}

// pausedBinding is one snapshot entry of the collector's pause states.
type pausedBinding struct {
	source string
	lane   string
	// reason is the binding's active pause reason; "" means not paused.
	reason string
}

// pausedCollector publishes courier_source_discovery_paused. The current pause
// reason per binding lives in one mutex-protected map, and a scrape takes a
// single consistent snapshot of that map before emitting: the gauge therefore
// never shows two reasons at 1 for one binding, and never shows a
// half-applied reason transition.
type pausedCollector struct {
	mu      sync.Mutex
	reasons map[bindingKey]string
}

// newPausedCollector builds the paused-state collector.
func newPausedCollector() *pausedCollector {
	return &pausedCollector{reasons: map[bindingKey]string{}}
}

// Describe implements prometheus.Collector.
func (c *pausedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pausedDesc
}

// Collect implements prometheus.Collector. It emits one series per
// (binding, reason) of the closed reason vocabulary: 1 for the binding's
// active reason and 0 for every other reason, all 0 when the binding is not
// paused.
func (c *pausedCollector) Collect(ch chan<- prometheus.Metric) {
	for _, binding := range c.snapshot() {
		for _, reason := range DiscoveryPauseReasons {
			value := 0.0
			if binding.reason == reason {
				value = 1
			}
			ch <- prometheus.MustNewConstMetric(pausedDesc, prometheus.GaugeValue, value, binding.source, binding.lane, reason)
		}
	}
}

// set records the binding's current pause reason; "" means not paused. One
// write under the lock is the whole transition: there is no window in which
// the binding's old and new reason are both live.
func (c *pausedCollector) set(source, lane, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reasons[bindingKey{source: source, lane: lane}] = reason
}

// snapshot copies the pause states under the lock, so the emission phase runs
// without holding it and every scrape sees one consistent state.
func (c *pausedCollector) snapshot() []pausedBinding {
	c.mu.Lock()
	defer c.mu.Unlock()
	states := make([]pausedBinding, 0, len(c.reasons))
	for key, reason := range c.reasons {
		states = append(states, pausedBinding{source: key.source, lane: key.lane, reason: reason})
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].source != states[j].source {
			return states[i].source < states[j].source
		}
		return states[i].lane < states[j].lane
	})
	return states
}
