package source

import "github.com/prometheus/client_golang/prometheus"

// DiscoverySkipReasons is the closed vocabulary of reasons the runner drops an
// offered item during validation. Tests reference this slice as the single
// source of truth.
var DiscoverySkipReasons = []string{"missing-id", "unsupported-mode", "invalid-ref"}

// DiscoveryPauseReasons is the closed vocabulary of reasons a poll is skipped
// before the source is called. capacity-cooldown was added by #327 after the
// design text was written; the vocabulary stays closed — new reasons require
// updating this slice, not free-form strings at emission sites.
var DiscoveryPauseReasons = []string{"lane-waiting", "lane-suspended", "lane-at-capacity", "capacity-cooldown"}

// DiscoveryMetrics is the discovery and lane-capacity signal layer for one
// process (§3.2 of docs/observability-signals-and-health.md). Every emission
// method is nil-safe: a Runner built without Metrics emits nothing, so the
// zero-value runner keeps its old behaviour.
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
	paused       *prometheus.GaugeVec
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
		paused: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "courier_source_discovery_paused",
			Help: "1 while the binding is currently paused before calling the source, 0 otherwise; exactly one reason is 1 per paused binding.",
		}, []string{"source", "lane", "reason"}),
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
// gauge series is 1 and every other reason's is 0. Multiple bindings may share
// one (source, lane) pair, so the gauge reflects the most recent poll's
// evaluation of that pair's lane-wide pause state; every runner re-evaluates
// the same lane-scoped gates each poll, so the value self-corrects within one
// poll interval.
func (m *DiscoveryMetrics) SetPaused(source, lane, reason string) {
	if m == nil {
		return
	}
	m.pausedTotal.WithLabelValues(source, lane, reason).Inc()
	m.setPausedGauge(source, lane, reason, 1)
}

// ClearPaused marks the binding not paused: every reason's gauge series is 0.
func (m *DiscoveryMetrics) ClearPaused(source, lane string) {
	if m == nil {
		return
	}
	m.setPausedGauge(source, lane, "", 0)
}

// setPausedGauge zeroes every non-active reason before raising the active one,
// so a scrape racing a reason transition never observes two reasons at 1.
func (m *DiscoveryMetrics) setPausedGauge(source, lane, activeReason string, value float64) {
	for _, reason := range DiscoveryPauseReasons {
		if reason != activeReason {
			m.paused.WithLabelValues(source, lane, reason).Set(0)
		}
	}
	if activeReason != "" {
		m.paused.WithLabelValues(source, lane, activeReason).Set(value)
	}
}
