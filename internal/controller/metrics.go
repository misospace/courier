package controller

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Run duration buckets are in seconds. The 14400s (4h) ceiling covers the
// full resolve-issue timeout plus tail; everything below is fine-grained so
// a slow-but-successful run is distinguishable from a timed-out one.
var runDurationBuckets = []float64{
	60, 120, 300, 600, 900, 1800, 2700, 3600, 5400, 7200, 10800, 14400,
}

// Queue wait buckets are in seconds. The distribution is front-loaded: most
// runs are admitted within a few seconds of creation, and the tail marks the
// admission-saturation cases (lane full, lane suspended).
var queueWaitBuckets = []float64{
	1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600,
}

// The label sets are shared between the metric constructors and the
// cardinality guard test so both reference one source of truth.
var (
	terminalMetricLabels = []string{"lane", "mode", "phase"}
	laneMetricLabels     = []string{"lane"}
	reapMetricLabels     = []string{"phase"}
)

var (
	runDurationDesc = prometheus.NewDesc(
		"courier_coderun_run_duration_seconds",
		"Duration of a CoderRun from StartedAt to FinishedAt, by lane, mode, and terminal phase.",
		terminalMetricLabels,
		nil,
	)
	queueWaitDesc = prometheus.NewDesc(
		"courier_coderun_queue_wait_seconds",
		"Time a CoderRun spent waiting for admission, from creation to AdmittedAt, by lane, mode, and terminal phase.",
		terminalMetricLabels,
		nil,
	)
	runsTotalDesc = prometheus.NewDesc(
		"courier_coderuns_total",
		"Total CoderRuns that reached a terminal phase, by lane, mode, and terminal phase.",
		terminalMetricLabels,
		nil,
	)
	inFlightDesc = prometheus.NewDesc(
		"courier_coderuns_in_flight",
		"CoderRuns per lane that currently consume the lane's admission capacity.",
		laneMetricLabels,
		nil,
	)
	laneSuspendedDesc = prometheus.NewDesc(
		"courier_lane_suspended",
		"Whether a lane is suspended from new work (1) or not (0).",
		laneMetricLabels,
		nil,
	)
	oldestTerminalAgeDesc = prometheus.NewDesc(
		"courier_coderuns_oldest_terminal_age_seconds",
		"Age of the oldest retained terminal CoderRun, by terminal phase.",
		reapMetricLabels,
		nil,
	)
	laneConcurrencyDesc = prometheus.NewDesc(
		"courier_lane_concurrency",
		"Configured LaneProfile concurrency slots; a non-positive setting is reported as the runner's default of 1.",
		laneMetricLabels,
		nil,
	)
	laneReservationsDesc = prometheus.NewDesc(
		"courier_lane_reservations",
		"CoderRuns per lane that currently reserve discovery capacity, including not-yet-admitted Pending and empty-phase runs.",
		laneMetricLabels,
		nil,
	)
	coderunsPendingDesc = prometheus.NewDesc(
		"courier_coderuns_pending",
		"CoderRuns per lane in PhasePending, the queue display subset of lane reservations.",
		laneMetricLabels,
		nil,
	)
	laneOldestReservationAgeDesc = prometheus.NewDesc(
		"courier_lane_oldest_reservation_age_seconds",
		"Age of the oldest CoderRun reserving discovery capacity on the lane; absent when there are none.",
		laneMetricLabels,
		nil,
	)
)

// RunRecorder observes CoderRuns as they reach their terminal phase. The
// three collectors are registered on the given registerer; a lazy process
// default bound to the controller-runtime metrics registry is used when a
// reconciler has no explicit recorder (see CoderRunReconciler.metrics).
type RunRecorder struct {
	RunDuration  *prometheus.HistogramVec
	QueueWait    *prometheus.HistogramVec
	Total        *prometheus.CounterVec
	ReapRequests *prometheus.CounterVec
}

func NewRunRecorder(reg prometheus.Registerer) *RunRecorder {
	rec := &RunRecorder{
		RunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "courier_coderun_run_duration_seconds",
			Help:    "Duration of a CoderRun from StartedAt to FinishedAt, by lane, mode, and terminal phase.",
			Buckets: runDurationBuckets,
		}, terminalMetricLabels),
		QueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "courier_coderun_queue_wait_seconds",
			Help:    "Time a CoderRun spent waiting for admission, from creation to AdmittedAt, by lane, mode, and terminal phase.",
			Buckets: queueWaitBuckets,
		}, terminalMetricLabels),
		Total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_coderuns_total",
			Help: "Total CoderRuns that reached a terminal phase, by lane, mode, and terminal phase.",
		}, terminalMetricLabels),
		ReapRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_coderuns_reap_requests_total",
			Help: "Terminal CoderRun deletions requested by the retention reaper, by terminal phase. The API server accepts a request even while a finalizer still holds the object; courier_coderuns_oldest_terminal_age_seconds shows what is actually retained.",
		}, reapMetricLabels),
	}
	reg.MustRegister(rec.RunDuration, rec.QueueWait, rec.Total, rec.ReapRequests)
	return rec
}

var (
	defaultRunRecorderOnce sync.Once
	defaultRunRecorder     *RunRecorder
)

// processRunRecorder lazily builds the process-wide recorder bound to the
// controller-runtime metrics registry, which the /metrics endpoint already
// serves.
func processRunRecorder() *RunRecorder {
	defaultRunRecorderOnce.Do(func() {
		defaultRunRecorder = NewRunRecorder(crmetrics.Registry)
	})
	return defaultRunRecorder
}

// ObserveTerminal records a CoderRun that has just entered its terminal
// phase. It is called exactly once per run: the reconciler records metrics on
// the run's first terminal transition, so a later transition that corrects
// the ending — a confirmed merge settling AwaitingReview as Done (#250) —
// does not count the run twice.
func (r *RunRecorder) ObserveTerminal(run *courierv1alpha1.CoderRun) {
	if run == nil {
		return
	}
	labels := prometheus.Labels{
		"lane":  run.Spec.Lane,
		"mode":  string(run.Spec.Mode),
		"phase": string(run.Status.Phase),
	}
	r.Total.With(labels).Inc()

	if run.Status.StartedAt != nil && run.Status.FinishedAt != nil {
		r.RunDuration.With(labels).Observe(nonNegativeSeconds(
			run.Status.FinishedAt.Time.Sub(run.Status.StartedAt.Time)))
	}
	if run.Status.AdmittedAt != nil {
		r.QueueWait.With(labels).Observe(nonNegativeSeconds(
			run.Status.AdmittedAt.Time.Sub(run.CreationTimestamp.Time)))
	}
}

// ObserveReapRequest records an accepted deletion request for a terminal
// CoderRun. It is deliberately a request counter, not a "gone" counter: the
// Kubernetes API server accepts a delete on a secure run while the topology
// finalizer still holds the object, so the object may outlive the counter.
// Completed cleanup is observable instead through the scrape-time
// courier_coderuns_oldest_terminal_age_seconds gauge, which reads live state.
// Requests are counted once per delete attempt and keyed only by the run's
// terminal phase, so a reaped run leaves one bounded, aggregatable trace.
func (r *RunRecorder) ObserveReapRequest(phase courierv1alpha1.Phase) {
	if r == nil || r.ReapRequests == nil {
		return
	}
	r.ReapRequests.With(prometheus.Labels{"phase": string(phase)}).Inc()
}

// nonNegativeSeconds clamps a duration to zero for the negative case, which
// would otherwise be a clock-skew artifact. Negative samples are nonsensical
// for these distributions, so they are folded into the first bucket.
func nonNegativeSeconds(d time.Duration) float64 {
	if d < 0 {
		return 0
	}
	return d.Seconds()
}

// LaneCollector reports per-lane CoderRun capacity usage and lane suspension
// by reading the live cluster at scrape time. Every LaneProfile is reported on
// both gauges, with drained lanes carrying a zero in-flight count, and a
// failed List increments the scrape-errors counter instead of silently
// dropping the scrape. Capacity consumption follows phaseConsumesCapacity:
// only Claimed and Running runs count, so a Verifying run no longer blocks new
// admissions. The discovery-side reservation gauges are deliberately broader:
// source.PhaseReservesDiscovery also counts Pending and empty-phase runs, so a
// binding does not hoard materialized-but-unadmitted work.
type LaneCollector struct {
	client       client.Client
	now          func() time.Time
	scrapeErrors prometheus.Counter
}

func NewLaneCollector(c client.Client) *LaneCollector {
	return &LaneCollector{
		client: c,
		now:    time.Now,
		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "courier_metrics_scrape_errors_total",
			Help: "List calls that failed while the LaneCollector collected lane metrics.",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *LaneCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- inFlightDesc
	ch <- laneSuspendedDesc
	ch <- laneConcurrencyDesc
	ch <- laneReservationsDesc
	ch <- coderunsPendingDesc
	ch <- laneOldestReservationAgeDesc
	ch <- c.scrapeErrors.Desc()
}

// Collect implements prometheus.Collector.
func (c *LaneCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() { ch <- c.scrapeErrors }()

	lanes := &courierv1alpha1.LaneProfileList{}
	if err := c.client.List(ctx, lanes); err != nil {
		c.scrapeErrors.Inc()
		crlog.Log.Error(err, "metrics scrape: listing LaneProfiles failed")
		return
	}
	// Seed every lane at zero so a lane whose runs all drained keeps
	// reporting 0 instead of disappearing from the series.
	inFlight := make(map[string]int, len(lanes.Items))
	reservations := make(map[string]int, len(lanes.Items))
	pending := make(map[string]int, len(lanes.Items))
	concurrency := make(map[string]float64, len(lanes.Items))
	for i := range lanes.Items {
		lane := lanes.Items[i].Name
		inFlight[lane] = 0
		reservations[lane] = 0
		pending[lane] = 0
		slots := lanes.Items[i].Spec.Concurrency
		if slots <= 0 {
			slots = 1
		}
		concurrency[lane] = float64(slots)
	}

	runs := &courierv1alpha1.CoderRunList{}
	if err := c.client.List(ctx, runs); err != nil {
		c.scrapeErrors.Inc()
		crlog.Log.Error(err, "metrics scrape: listing CoderRuns failed")
		return
	}
	oldestReservation := make(map[string]time.Time)
	for i := range runs.Items {
		run := &runs.Items[i]
		if phaseConsumesCapacity(run.Status.Phase) {
			inFlight[run.Spec.Lane]++
		}
		if source.PhaseReservesDiscovery(run.Status.Phase) {
			reservations[run.Spec.Lane]++
			if current, ok := oldestReservation[run.Spec.Lane]; !ok || run.CreationTimestamp.Time.Before(current) {
				oldestReservation[run.Spec.Lane] = run.CreationTimestamp.Time
			}
		}
		if run.Status.Phase == courierv1alpha1.PhasePending {
			pending[run.Spec.Lane]++
		}
	}
	for lane, count := range inFlight {
		ch <- prometheus.MustNewConstMetric(
			inFlightDesc, prometheus.GaugeValue, float64(count), lane)
	}
	for lane, slots := range concurrency {
		ch <- prometheus.MustNewConstMetric(
			laneConcurrencyDesc, prometheus.GaugeValue, slots, lane)
	}
	for lane, count := range reservations {
		ch <- prometheus.MustNewConstMetric(
			laneReservationsDesc, prometheus.GaugeValue, float64(count), lane)
	}
	for lane, count := range pending {
		ch <- prometheus.MustNewConstMetric(
			coderunsPendingDesc, prometheus.GaugeValue, float64(count), lane)
	}
	// A lane with no reservations reports no age series: absence is "no
	// reservation", never a zero age.
	now := c.now()
	for lane, at := range oldestReservation {
		ch <- prometheus.MustNewConstMetric(
			laneOldestReservationAgeDesc, prometheus.GaugeValue, nonNegativeSeconds(now.Sub(at)), lane)
	}

	for i := range lanes.Items {
		suspended := 0.0
		if lanes.Items[i].Suspended() {
			suspended = 1
		}
		ch <- prometheus.MustNewConstMetric(
			laneSuspendedDesc, prometheus.GaugeValue, suspended, lanes.Items[i].Name)
	}
}

// TerminalAgeCollector reports the age of the oldest retained terminal
// CoderRun per phase, read from the live cluster at scrape time. It is the
// observability half of the retention reaper (#287): the reap-request counter
// shows deletions the API server accepted, and this gauge shows what is
// actually retained — an object a finalizer still holds stays on it, so a
// request that never completed cleanup is visible. It also shows whether
// anything is aging past its window — a stuck lifecycle report or an
// audit-floor hold. A failed List increments the scrape-errors counter instead
// of silently dropping the scrape.
type TerminalAgeCollector struct {
	client       client.Client
	now          func() time.Time
	scrapeErrors prometheus.Counter
}

func NewTerminalAgeCollector(c client.Client) *TerminalAgeCollector {
	return &TerminalAgeCollector{
		client: c,
		now:    time.Now,
		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "courier_terminal_age_scrape_errors_total",
			Help: "List calls that failed while the TerminalAgeCollector collected terminal-age metrics.",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *TerminalAgeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- oldestTerminalAgeDesc
	ch <- c.scrapeErrors.Desc()
}

// Collect implements prometheus.Collector.
func (c *TerminalAgeCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() { ch <- c.scrapeErrors }()

	var runs courierv1alpha1.CoderRunList
	if err := c.client.List(ctx, &runs); err != nil {
		c.scrapeErrors.Inc()
		crlog.Log.Error(err, "metrics scrape: listing CoderRuns failed")
		return
	}
	oldest := make(map[courierv1alpha1.Phase]time.Time, 6)
	for i := range runs.Items {
		run := &runs.Items[i]
		if !terminalPhase(run.Status.Phase) {
			continue
		}
		at := terminalTime(run)
		if at.IsZero() {
			continue
		}
		if current, ok := oldest[run.Status.Phase]; !ok || at.Before(current) {
			oldest[run.Status.Phase] = at
		}
	}
	now := c.now()
	for phase, at := range oldest {
		ch <- prometheus.MustNewConstMetric(
			oldestTerminalAgeDesc, prometheus.GaugeValue, nonNegativeSeconds(now.Sub(at)), string(phase))
	}
}
