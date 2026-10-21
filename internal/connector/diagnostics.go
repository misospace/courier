// Package connector implements the core-defined, connector-agnostic
// diagnostic envelope of docs/connector-diagnostics.md (§2, §6): one bounded
// courier_connector_* metric family set and one connector.diagnostic
// structured log event, carried by a single Emitter. A connector declares its
// closed reason vocabulary per signal at registration; every unrecognised
// reason folds onto a single "other" value at the boundary so metric
// cardinality cannot grow with upstream data. Courier core never interprets
// the vocabulary values.
package connector

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/misospace/courier/internal/log"
)

// Signal names of the closed core vocabulary (§3). A signal is a bounded
// diagnostic a declared capability may emit; connectors reference these
// constants, never raw strings.
const (
	SignalExclusion     = "exclusion"
	SignalAck           = "ack"
	SignalProtocolError = "protocol-error"
	SignalReport        = "report"
	SignalRevalidation  = "revalidation"
	SignalIdentity      = "identity"
	SignalAvailability  = "availability"
)

// Signals is the closed core signal vocabulary. It is the single
// source of truth for the envelope's signal label values.
var Signals = []string{
	SignalExclusion,
	SignalAck,
	SignalProtocolError,
	SignalReport,
	SignalRevalidation,
	SignalIdentity,
	SignalAvailability,
}

// AvailabilityStates are the connector-declared availability states (§3.5).
// available is the only success state; unsupported and unknown are distinct
// from both available and unreachable.
const (
	AvailabilityAvailable   = "available"
	AvailabilityDegraded    = "degraded"
	AvailabilityUnreachable = "unreachable"
	AvailabilityUnsupported = "unsupported"
	AvailabilityUnknown     = "unknown"
)

// AvailabilityStates is the closed availability state vocabulary.
var AvailabilityStates = []string{
	AvailabilityAvailable,
	AvailabilityDegraded,
	AvailabilityUnreachable,
	AvailabilityUnsupported,
	AvailabilityUnknown,
}

// ReasonOther replaces every unrecognised reason value at the boundary (§6.1).
const ReasonOther = "other"

// ReasonOverflow is the catalogue-defined exclusion overflow reason (§3.1).
// Connectors declaring an exclusion vocabulary must include it so overflow
// cycles count under reason=overflow rather than folding to other.
const ReasonOverflow = "overflow"

// Metric family names registered by this package.
const (
	MetricDiagnosticsTotal   = "courier_connector_diagnostics_total"
	MetricExclusionRequested = "courier_connector_exclusion_requested"
	MetricExclusionAccepted  = "courier_connector_exclusion_accepted"
	MetricExclusionOverflow  = "courier_connector_exclusion_overflow"
	MetricExclusionPressure  = "courier_connector_exclusion_pressure"
	MetricExclusionLimit     = "courier_connector_exclusion_limit"
)

// LabelKeysAllowlist is the closed set of label keys any courier_connector_*
// family may use (§11.1). Numeric observations travel as metric values, so
// requested, limit, accepted, and pressure are deliberately absent.
var LabelKeysAllowlist = []string{
	"connector",
	"mode",
	"lane",
	"phase",
	"signal",
	"reason",
	"state",
	"capability",
}

// CorrelationKeys are the identifiers that ride the log half of the envelope
// only (§6.4). They are unbounded and must never become metric labels.
var CorrelationKeys = []string{"run_id", "trace_id", "span_id"}

// ErrCorrelationLabel is returned by RejectCorrelation when a payload
// intended for the metric half carries a correlation identifier.
var ErrCorrelationLabel = errors.New("connector: correlation identifiers ride the log half of the envelope, never metric labels")

// RejectCorrelation reports an ErrCorrelationLabel error wrapping the offending
// key when payload carries any correlation identifier. It is the metric half's
// correlation guard: every metric write path in this package runs its label
// set through it before touching a collector, and tests call it directly.
func RejectCorrelation(payload map[string]string) error {
	for _, key := range CorrelationKeys {
		if _, ok := payload[key]; ok {
			return fmt.Errorf("%w: %q", ErrCorrelationLabel, key)
		}
	}
	return nil
}

// CheckLabelKeys reports an error when any label key of a metric family is
// outside LabelKeysAllowlist. NewEmitter runs it over every family it
// registers, and the package tests run it over every family gathered from a
// live registry, so a label key that slips in fails at construction and in CI.
func CheckLabelKeys(family string, keys ...string) error {
	for _, key := range keys {
		if !slices.Contains(LabelKeysAllowlist, key) {
			return fmt.Errorf("connector: metric family %q uses label key %q outside the allowlist", family, key)
		}
	}
	return nil
}

// envelopeFamily pairs a registered metric family with its label keys so
// registration validates them against the allowlist in one place.
type envelopeFamily struct {
	name string
	keys []string
}

var envelopeFamilies = []envelopeFamily{
	{MetricDiagnosticsTotal, []string{"connector", "signal", "reason"}},
	{MetricExclusionRequested, []string{"connector"}},
	{MetricExclusionAccepted, []string{"connector"}},
	{MetricExclusionOverflow, []string{"connector"}},
	{MetricExclusionPressure, []string{"connector"}},
	{MetricExclusionLimit, []string{"connector"}},
}

// EventConnectorDiagnostic is the envelope's log event type (§2.2).
const EventConnectorDiagnostic = "connector.diagnostic"

// DiagnosticEvent is one diagnostic crossing the envelope's log half.
// Connector, Signal, and Reason are bounded values already normalised at the
// boundary. Detail is free-form diagnostic text: the sink must redact it
// through internal/log's redactor and include it only at debug verbosity —
// it never reaches the metric half. RunID is optional correlation (§6.4).
type DiagnosticEvent struct {
	Connector string
	Signal    string
	Reason    string
	Detail    string
	RunID     string
}

// DiagnosticLogSink receives the log half of the diagnostic envelope.
// Implementations must redact DiagnosticEvent.Detail through
// internal/log's redactor before writing it and must include it only at
// debug verbosity; the metric half never sees detail.
type DiagnosticLogSink interface {
	EmitDiagnostic(event DiagnosticEvent)
}

// EventLogSink adapts internal/log's structured event emitter as a
// DiagnosticLogSink. The bounded connector/signal/reason values ride the
// event's detail object, which the emitter serialises on every line (Verbose);
// the free-form detail is added only when the emitter runs at debug level and
// is redacted through the emitter's redactor first — the last-line guard
// remains internal/log's redactor. Deployment secrets must be registered on
// that redactor via Register and RegisterEnvironment.
type EventLogSink struct {
	emitter *log.Emitter
}

// NewEventLogSink adapts the given structured event emitter. A nil emitter
// discards the log half.
func NewEventLogSink(emitter *log.Emitter) *EventLogSink {
	return &EventLogSink{emitter: emitter}
}

// Emitter returns the underlying structured event emitter.
func (s *EventLogSink) Emitter() *log.Emitter {
	if s == nil {
		return nil
	}
	return s.emitter
}

// unscopedRunID stands in for run identity on connector-scoped diagnostics
// that have no run to correlate to; internal/log requires every line to
// carry a run_id so a log store can filter to a run.
const unscopedRunID = "connector"

// EmitDiagnostic writes one connector.diagnostic event line.
func (s *EventLogSink) EmitDiagnostic(event DiagnosticEvent) {
	if s == nil || s.emitter == nil {
		return
	}
	detail := map[string]any{
		"connector": event.Connector,
		"signal":    event.Signal,
		"reason":    event.Reason,
	}
	if event.Detail != "" && s.emitter.Level() == log.LevelDebug {
		detail["detail"] = s.emitter.Redactor().Redact(event.Detail)
	}
	runID := event.RunID
	if strings.TrimSpace(runID) == "" {
		runID = unscopedRunID
	}
	// Event emission is best-effort throughout internal/log.
	_ = s.emitter.Emit(log.Event{
		Type:    EventConnectorDiagnostic,
		RunID:   runID,
		Verbose: true,
		Detail:  detail,
	})
}

// emitterState holds the per-connector registration data behind a pointer so
// an Emitter view (see WithRunID) can be derived by value without copying a
// lock.
type emitterState struct {
	mu     sync.RWMutex
	vocab  map[string]map[string]map[string]struct{}
	limits map[string]int
}

// Emitter carries the envelope's two halves: the metric families registered
// on a caller-supplied registry, and a DiagnosticLogSink for the structured
// log events. It is safe for concurrent use; a zero Emitter discards events.
type Emitter struct {
	sink  DiagnosticLogSink
	runID string
	state *emitterState

	diagnosticsTotal   *prometheus.CounterVec
	capabilityInfo     *prometheus.GaugeVec
	exclusionRequested *prometheus.GaugeVec
	exclusionAccepted  *prometheus.GaugeVec
	exclusionOverflow  *prometheus.GaugeVec
	exclusionPressure  *prometheus.GaugeVec
	exclusionLimit     *prometheus.GaugeVec
}

// NewEmitter constructs an Emitter whose metric families are registered on
// reg and whose log half is written to sink (a nil sink keeps the metric half
// only). Every family's label keys are validated against LabelKeysAllowlist
// at registration time.
func NewEmitter(reg prometheus.Registerer, sink DiagnosticLogSink) *Emitter {
	for _, family := range envelopeFamilies {
		if err := CheckLabelKeys(family.name, family.keys...); err != nil {
			panic(err)
		}
	}
	capabilityInfo := newCapabilityInfo()
	e := &Emitter{
		sink:           sink,
		state:          &emitterState{vocab: map[string]map[string]map[string]struct{}{}, limits: map[string]int{}},
		capabilityInfo: capabilityInfo,
		diagnosticsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: MetricDiagnosticsTotal,
			Help: "Connector diagnostic events, by connector, signal, and connector-declared reason.",
		}, []string{"connector", "signal", "reason"}),
		exclusionRequested: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: MetricExclusionRequested,
			Help: "Unique valid exclusion descriptors the connector intended to send in the last discovery cycle.",
		}, []string{"connector"}),
		exclusionAccepted: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: MetricExclusionAccepted,
			Help: "Exclusion descriptors the connector actually sent in the last discovery cycle.",
		}, []string{"connector"}),
		exclusionOverflow: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: MetricExclusionOverflow,
			Help: "Exclusion descriptors dropped or refused because requested exceeded the limit in the last discovery cycle.",
		}, []string{"connector"}),
		exclusionPressure: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: MetricExclusionPressure,
			Help: "Requested exclusion descriptors divided by the configured limit; 0 when the limit is unknown or nothing was requested.",
		}, []string{"connector"}),
		exclusionLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: MetricExclusionLimit,
			Help: "Connector-declared exclusion contract maximum, exported as data.",
		}, []string{"connector"}),
	}
	reg.MustRegister(
		e.diagnosticsTotal,
		e.capabilityInfo,
		e.exclusionRequested,
		e.exclusionAccepted,
		e.exclusionOverflow,
		e.exclusionPressure,
		e.exclusionLimit,
	)
	return e
}

// WithRunID returns a view of the emitter whose log-half events carry the
// given CoderRun name for correlation (§6.4). The view shares the metric
// collectors and registration state with the receiver; correlation never
// reaches the metric half.
func (e *Emitter) WithRunID(runID string) *Emitter {
	if e == nil {
		return nil
	}
	view := *e
	view.runID = runID
	return &view
}

// RegisterConnector registers a connector's closed reason vocabulary per
// signal (§6.1). Registering the same connector or reason twice raises no
// error; registering vocabulary for a signal outside the closed core
// vocabulary panics, because a connector cannot emit a signal Courier has no
// seam for.
func (e *Emitter) RegisterConnector(name string, vocab map[string][]string) {
	if e == nil {
		return
	}
	for signal := range vocab {
		if !slices.Contains(Signals, signal) {
			panic(fmt.Sprintf("connector: %q registers vocabulary for unknown signal %q", name, signal))
		}
	}
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	perConnector, ok := e.state.vocab[name]
	if !ok {
		perConnector = map[string]map[string]struct{}{}
		e.state.vocab[name] = perConnector
	}
	for signal, reasons := range vocab {
		perSignal, ok := perConnector[signal]
		if !ok {
			perSignal = map[string]struct{}{}
			perConnector[signal] = perSignal
		}
		for _, reason := range reasons {
			perSignal[reason] = struct{}{}
		}
	}
}

// Diagnostic records one connector diagnostic event: it folds an
// unrecognised reason (or signal) onto the single other catch-all, increments
// courier_connector_diagnostics_total once, and emits the connector.diagnostic
// log event. A signal outside the closed core vocabulary folds onto the same
// catch-all so a misbehaving caller cannot grow cardinality.
func (e *Emitter) Diagnostic(connector, signal, reason string) {
	e.diagnosticDetail(connector, signal, reason, "")
}

// DiagnosticDetail is Diagnostic plus free-form detail text. The detail is
// redacted by the log sink and included only at debug verbosity; the metric
// half never sees it.
func (e *Emitter) DiagnosticDetail(connector, signal, reason, detail string) {
	e.diagnosticDetail(connector, signal, reason, detail)
}

func (e *Emitter) diagnosticDetail(connector, signal, reason, detail string) {
	if e == nil {
		return
	}
	if !slices.Contains(Signals, signal) {
		signal = ReasonOther
	}
	reason = e.resolveReason(connector, signal, reason)
	labels := prometheus.Labels{"connector": connector, "signal": signal, "reason": reason}
	if err := RejectCorrelation(labels); err != nil {
		return
	}
	e.diagnosticsTotal.With(labels).Inc()
	if e.sink != nil {
		e.sink.EmitDiagnostic(DiagnosticEvent{
			Connector: connector,
			Signal:    signal,
			Reason:    reason,
			Detail:    detail,
			RunID:     e.runID,
		})
	}
}

// ExclusionCycle records one discovery cycle in which exclusions were
// attempted (§3.1): requested, accepted, and overflow (max(0,
// requested-accepted)) are set as gauge values, and pressure is set to
// requested/limit, or 0 when the limit is unknown or nothing was requested.
// When overflow is positive the overflow diagnostic is counted exactly once
// for the cycle — the dropped count lives in the gauge value, not in extra
// events — and no overflow event is counted at all when nothing was
// requested.
func (e *Emitter) ExclusionCycle(connector string, requested, accepted int) {
	if e == nil {
		return
	}
	overflow := requested - accepted
	if overflow < 0 {
		overflow = 0
	}
	e.exclusionRequested.WithLabelValues(connector).Set(float64(requested))
	e.exclusionAccepted.WithLabelValues(connector).Set(float64(accepted))
	e.exclusionOverflow.WithLabelValues(connector).Set(float64(overflow))
	pressure := 0.0
	if limit := e.limit(connector); limit > 0 && requested > 0 {
		pressure = float64(requested) / float64(limit)
	}
	e.exclusionPressure.WithLabelValues(connector).Set(pressure)
	if overflow > 0 {
		e.diagnosticDetail(connector, SignalExclusion, ReasonOverflow, "")
	}
}

// ExclusionLimit exports the connector's exclusion contract maximum as data
// (§3.1). It feeds the pressure ratio on subsequent ExclusionCycle calls.
// A connector is identified by its bounded label, so redeclaring a different
// limit for an already-declared connector is a configuration conflict that
// would silently clobber the exported gauge; it fails loudly instead.
func (e *Emitter) ExclusionLimit(connector string, limit int) {
	if e == nil {
		return
	}
	e.state.mu.Lock()
	if prev, seen := e.state.limits[connector]; seen && prev != limit {
		e.state.mu.Unlock()
		panic(fmt.Sprintf("connector diagnostics: %q redeclared exclusion limit %d after %d", connector, limit, prev))
	}
	e.state.limits[connector] = limit
	e.state.mu.Unlock()
	e.exclusionLimit.WithLabelValues(connector).Set(float64(limit))
}

// Availability records the connector-declared availability state (§3.5) as an
// availability diagnostic, validated against the connector's registered
// availability vocabulary.
func (e *Emitter) Availability(connector, state string) {
	e.Diagnostic(connector, SignalAvailability, state)
}

func (e *Emitter) limit(connector string) int {
	e.state.mu.RLock()
	defer e.state.mu.RUnlock()
	return e.state.limits[connector]
}

func (e *Emitter) resolveReason(connector, signal, reason string) string {
	e.state.mu.RLock()
	defer e.state.mu.RUnlock()
	if perSignal, ok := e.state.vocab[connector][signal]; ok {
		if _, ok := perSignal[reason]; ok {
			return reason
		}
	}
	return ReasonOther
}
