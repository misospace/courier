package controller

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
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
)

// RunRecorder observes CoderRuns as they reach their terminal phase. The
// three collectors are registered on the given registerer; a lazy process
// default bound to the controller-runtime metrics registry is used when a
// reconciler has no explicit recorder (see CoderRunReconciler.metrics).
type RunRecorder struct {
	RunDuration *prometheus.HistogramVec
	QueueWait   *prometheus.HistogramVec
	Total       *prometheus.CounterVec
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
	}
	reg.MustRegister(rec.RunDuration, rec.QueueWait, rec.Total)
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
// phase. It is called exactly once per run: the terminal phase is final, so
// re-reconciles of an already-terminal run are no-ops here.
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
// admissions.
type LaneCollector struct {
	client       client.Client
	scrapeErrors prometheus.Counter
}

func NewLaneCollector(c client.Client) *LaneCollector {
	return &LaneCollector{
		client: c,
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
	for i := range lanes.Items {
		inFlight[lanes.Items[i].Name] = 0
	}

	runs := &courierv1alpha1.CoderRunList{}
	if err := c.client.List(ctx, runs); err != nil {
		c.scrapeErrors.Inc()
		crlog.Log.Error(err, "metrics scrape: listing CoderRuns failed")
		return
	}
	for i := range runs.Items {
		if !phaseConsumesCapacity(runs.Items[i].Status.Phase) {
			continue
		}
		inFlight[runs.Items[i].Spec.Lane]++
	}
	for lane, count := range inFlight {
		ch <- prometheus.MustNewConstMetric(
			inFlightDesc, prometheus.GaugeValue, float64(count), lane)
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
