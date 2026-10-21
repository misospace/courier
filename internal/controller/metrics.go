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
	terminalMetricLabels     = []string{"lane", "mode", "phase"}
	laneMetricLabels         = []string{"lane"}
	reapMetricLabels         = []string{"phase"}
	transitionMetricLabels   = []string{"lane", "mode", "from", "to"}
	laneModeMetricLabels     = []string{"lane", "mode"}
	diagnosticMetricLabels   = []string{"lane", "mode", "category"}
	verificationMetricLabels = []string{"lane", "mode", "result"}
	tokenMetricLabels        = []string{"lane", "mode", "kind"}
	lifecycleMetricLabels    = []string{"source", "lane", "mode", "result"}
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
	awaitingReapDesc = prometheus.NewDesc(
		"courier_coderuns_awaiting_reap",
		"Terminal CoderRuns retained and not yet reaped, by terminal phase.",
		reapMetricLabels,
		nil,
	)
	lifecyclePendingDesc = prometheus.NewDesc(
		"courier_coderuns_lifecycle_pending",
		"Terminal CoderRuns whose lifecycle report has not settled, by lane.",
		laneMetricLabels,
		nil,
	)
	lifecyclePendingOldestDesc = prometheus.NewDesc(
		"courier_coderuns_lifecycle_pending_oldest_age_seconds",
		"Age of the oldest terminal CoderRun with an unsettled lifecycle report, from FinishedAt, by lane.",
		laneMetricLabels,
		nil,
	)
)

// RunRecorder observes CoderRuns as they reach their terminal phase. The
// three collectors are registered on the given registerer; a lazy process
// default bound to the controller-runtime metrics registry is used when a
// reconciler has no explicit recorder (see CoderRunReconciler.metrics).
type RunRecorder struct {
	RunDuration             *prometheus.HistogramVec
	QueueWait               *prometheus.HistogramVec
	Total                   *prometheus.CounterVec
	ReapRequests            *prometheus.CounterVec
	PhaseTransitions        *prometheus.CounterVec
	PhaseDuration           *prometheus.HistogramVec
	TerminalDiagnostics     *prometheus.CounterVec
	InfraRestarts           *prometheus.CounterVec
	VerificationOutcomes    *prometheus.CounterVec
	Tokens                  *prometheus.CounterVec
	LifecycleReportOutcomes *prometheus.CounterVec
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
		PhaseTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_coderun_phase_transitions_total",
			Help: "Observed CoderRun phase changes, by lane, mode, and the from/to phases.",
		}, transitionMetricLabels),
		PhaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "courier_coderun_phase_duration_seconds",
			Help:    "Time a CoderRun spent in a phase, observed once on leaving it, by lane, mode, and phase.",
			Buckets: runDurationBuckets,
		}, terminalMetricLabels),
		TerminalDiagnostics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_coderun_terminal_diagnostics_total",
			Help: "Terminal CoderRuns by their durable terminal diagnostic category, or unknown when none was recorded.",
		}, diagnosticMetricLabels),
		InfraRestarts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_coderun_infra_restarts_total",
			Help: "Coordinator infra relaunches charged to CoderRuns, by lane and mode.",
		}, laneModeMetricLabels),
		VerificationOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_coderun_verification_outcomes_total",
			Help: "External verification dispositions observed on Verifying exits, by lane, mode, and result (green, red, unobservable).",
		}, verificationMetricLabels),
		Tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_coderun_tokens_total",
			Help: "Aggregate CoderRun token usage from RunTelemetry, by lane, mode, and kind (input, output, reasoning, cache-read, cache-write).",
		}, tokenMetricLabels),
		LifecycleReportOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "courier_source_lifecycle_report_outcomes_total",
			Help: "Terminal source lifecycle report attempt outcomes, by source, lane, mode, and result (published, superseded, unsupported, retryable).",
		}, lifecycleMetricLabels),
	}
	reg.MustRegister(rec.RunDuration, rec.QueueWait, rec.Total, rec.ReapRequests,
		rec.PhaseTransitions, rec.PhaseDuration, rec.TerminalDiagnostics,
		rec.InfraRestarts, rec.VerificationOutcomes, rec.Tokens,
		rec.LifecycleReportOutcomes)
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

// Verification outcome values are a closed set derived from the observed
// Verifying exit, not free-form labels.
const (
	verificationOutcomeGreen        = "green"
	verificationOutcomeRed          = "red"
	verificationOutcomeUnobservable = "unobservable"

	lifecycleOutcomePublished   = "published"
	lifecycleOutcomeSuperseded  = "superseded"
	lifecycleOutcomeUnsupported = "unsupported"
	lifecycleOutcomeRetryable   = "retryable"
)

// Token kinds are the bounded TokenUsage fields; the derived Total is never
// emitted as its own series.
var tokenKinds = []string{"input", "output", "reasoning", "cache-read", "cache-write"}

// ObservePhaseTransition records one observed phase change. Callers skip
// empty phases so a legacy run recorded mid-lifecycle does not fabricate a
// transition from nothing.
func (r *RunRecorder) ObservePhaseTransition(lane string, mode courierv1alpha1.Mode, from, to courierv1alpha1.Phase) {
	if r == nil {
		return
	}
	r.PhaseTransitions.With(prometheus.Labels{
		"lane": lane,
		"mode": string(mode),
		"from": string(from),
		"to":   string(to),
	}).Inc()
}

// ObservePhaseDuration records the time a run spent in the phase it just
// left, observed once per transition.
func (r *RunRecorder) ObservePhaseDuration(lane string, mode courierv1alpha1.Mode, phase courierv1alpha1.Phase, d time.Duration) {
	if r == nil {
		return
	}
	r.PhaseDuration.With(prometheus.Labels{
		"lane":  lane,
		"mode":  string(mode),
		"phase": string(phase),
	}).Observe(nonNegativeSeconds(d))
}

// ObserveTerminalDiagnostic records one terminal run by its durable terminal
// diagnostic category; a run without a diagnostic counts under the closed
// "unknown" category.
func (r *RunRecorder) ObserveTerminalDiagnostic(run *courierv1alpha1.CoderRun) {
	if r == nil || run == nil {
		return
	}
	category := courierv1alpha1.TerminalDiagnosticCategoryUnknown
	if run.Status.TerminalDiagnostic != nil && run.Status.TerminalDiagnostic.Category != "" {
		category = run.Status.TerminalDiagnostic.Category
	}
	r.TerminalDiagnostics.With(prometheus.Labels{
		"lane":     run.Spec.Lane,
		"mode":     string(run.Spec.Mode),
		"category": string(category),
	}).Inc()
}

// ObserveInfraRestarts records coordinator infra relaunches charged to a run
// as the delta observed on the run's status.
func (r *RunRecorder) ObserveInfraRestarts(lane string, mode courierv1alpha1.Mode, delta int) {
	if r == nil || delta <= 0 {
		return
	}
	r.InfraRestarts.With(prometheus.Labels{
		"lane": lane,
		"mode": string(mode),
	}).Add(float64(delta))
}

// ObserveVerificationOutcome records the external verification disposition
// of a Verifying exit.
func (r *RunRecorder) ObserveVerificationOutcome(lane string, mode courierv1alpha1.Mode, result string) {
	if r == nil {
		return
	}
	r.VerificationOutcomes.With(prometheus.Labels{
		"lane":   lane,
		"mode":   string(mode),
		"result": result,
	}).Inc()
}

// ObserveTokenDelta records a positive per-kind token delta from the run's
// telemetry. Zero and negative deltas are skipped: missing series are zero in
// PromQL, and the derived Total is never emitted.
func (r *RunRecorder) ObserveTokenDelta(lane string, mode courierv1alpha1.Mode, kind string, delta int64) {
	if r == nil || delta <= 0 {
		return
	}
	r.Tokens.With(prometheus.Labels{
		"lane": lane,
		"mode": string(mode),
		"kind": kind,
	}).Add(float64(delta))
}

// ObserveLifecycleReportOutcome records one freshly applied terminal source
// lifecycle report attempt outcome.
func (r *RunRecorder) ObserveLifecycleReportOutcome(sourceName, lane string, mode courierv1alpha1.Mode, result string) {
	if r == nil {
		return
	}
	r.LifecycleReportOutcomes.With(prometheus.Labels{
		"source": sourceName,
		"lane":   lane,
		"mode":   string(mode),
		"result": result,
	}).Inc()
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

// TerminalBacklogCollector reports the terminal-run backlog at scrape time:
// how many terminal runs the retention policy still holds, and how many of
// those carry an unsettled source lifecycle report — the pending-report
// backlog the retention reaper holds runs for — with the age of the oldest
// unsettled run per lane. It mirrors TerminalAgeCollector: a failed List
// increments the dedicated scrape-errors counter instead of silently
// dropping the scrape.
type TerminalBacklogCollector struct {
	client       client.Client
	now          func() time.Time
	scrapeErrors prometheus.Counter
}

func NewTerminalBacklogCollector(c client.Client) *TerminalBacklogCollector {
	return &TerminalBacklogCollector{
		client: c,
		now:    time.Now,
		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "courier_terminal_backlog_scrape_errors_total",
			Help: "List calls that failed while the TerminalBacklogCollector collected terminal-backlog metrics.",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *TerminalBacklogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- awaitingReapDesc
	ch <- lifecyclePendingDesc
	ch <- lifecyclePendingOldestDesc
	ch <- c.scrapeErrors.Desc()
}

// Collect implements prometheus.Collector.
func (c *TerminalBacklogCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() { ch <- c.scrapeErrors }()

	var runs courierv1alpha1.CoderRunList
	if err := c.client.List(ctx, &runs); err != nil {
		c.scrapeErrors.Inc()
		crlog.Log.Error(err, "metrics scrape: listing CoderRuns failed")
		return
	}
	awaitingReap := make(map[courierv1alpha1.Phase]int, 6)
	pending := make(map[string]int)
	oldest := make(map[string]time.Time)
	for i := range runs.Items {
		run := &runs.Items[i]
		if !terminalPhase(run.Status.Phase) {
			continue
		}
		awaitingReap[run.Status.Phase]++
		// Mirror the reaper's hold gate: only a positively settled report
		// (True with reason Published or Superseded) leaves the backlog, so a
		// True condition with an unexpected reason stays counted, exactly as
		// lifecycleReportSettled holds the run in place.
		if lifecycleReportSettled(run) {
			continue
		}
		pending[run.Spec.Lane]++
		if run.Status.FinishedAt == nil {
			continue
		}
		if current, ok := oldest[run.Spec.Lane]; !ok || run.Status.FinishedAt.Time.Before(current) {
			oldest[run.Spec.Lane] = run.Status.FinishedAt.Time
		}
	}
	now := c.now()
	for phase, count := range awaitingReap {
		ch <- prometheus.MustNewConstMetric(
			awaitingReapDesc, prometheus.GaugeValue, float64(count), string(phase))
	}
	for lane, count := range pending {
		ch <- prometheus.MustNewConstMetric(
			lifecyclePendingDesc, prometheus.GaugeValue, float64(count), lane)
	}
	for lane, at := range oldest {
		ch <- prometheus.MustNewConstMetric(
			lifecyclePendingOldestDesc, prometheus.GaugeValue, nonNegativeSeconds(now.Sub(at)), lane)
	}
}
