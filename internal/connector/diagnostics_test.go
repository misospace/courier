package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/misospace/courier/internal/log"
)

const testConnector = "acme"

// newTestEmitter builds an emitter on a fresh registry with a log sink
// writing to a buffer at the given level.
func newTestEmitter(t *testing.T, level log.Level) (*Emitter, *EventLogSink, *prometheus.Registry, *bytes.Buffer) {
	t.Helper()
	reg := prometheus.NewRegistry()
	buf := &bytes.Buffer{}
	sink := NewEventLogSink(log.NewEmitter(buf, level))
	em := NewEmitter(reg, sink)
	return em, sink, reg, buf
}

func lastLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[len(lines)-1] == "" {
		t.Fatalf("no log line written; buffer %q", buf.String())
	}
	line := lines[len(lines)-1]
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("log line is not JSON: %v (%q)", err, line)
	}
	return decoded
}

func findSeries(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (*dto.Metric, bool) {
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
				return m, true
			}
		}
	}
	return nil, false
}

func counterValue(t *testing.T, reg *prometheus.Registry, labels map[string]string) (float64, bool) {
	t.Helper()
	m, ok := findSeries(t, reg, MetricDiagnosticsTotal, labels)
	if !ok || m.GetCounter() == nil {
		return 0, false
	}
	return m.GetCounter().GetValue(), true
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	m, ok := findSeries(t, reg, name, labels)
	if !ok || m.GetGauge() == nil {
		return 0, false
	}
	return m.GetGauge().GetValue(), true
}

func seriesCount(t *testing.T, reg *prometheus.Registry, name string, match func(*dto.Metric) bool) int {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	count := 0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if match(m) {
				count++
			}
		}
	}
	return count
}

func labelValue(m *dto.Metric, key string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == key {
			return lp.GetValue()
		}
	}
	return ""
}

// fullVocab declares a closed reason vocabulary for every signal.
func fullVocab() map[string][]string {
	return map[string][]string{
		SignalExclusion:     {ReasonOverflow},
		SignalAck:           {"ok", "missing", "invalid", "version-unsupported"},
		SignalProtocolError: {"transport-error", "auth-error", "http-4xx", "http-5xx", "malformed-response", "unsupported-task-type", "invalid-descriptor"},
		SignalReport:        {"published", "superseded", "unsupported", "retryable"},
		SignalRevalidation:  {"stale", "current", "error"},
		SignalIdentity:      {"generation-advanced", "identity-superseded", "stale-head", "closed-unmerged", "claim-released"},
		SignalAvailability:  {"available", "degraded", "unreachable"},
	}
}

// TestMetricFamiliesWithinLabelAllowlist is the allowlist assertion (§11.1)
// and the numeric-as-value assertion: every family this package registers is
// gathered from a live registry, each family's label keys must sit inside
// LabelKeysAllowlist and match its documented set exactly, and no label value
// may carry a numeric observation.
func TestMetricFamiliesWithinLabelAllowlist(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	em.RegisterConnector(testConnector, fullVocab())
	em.Diagnostic(testConnector, SignalProtocolError, "http-5xx")
	em.DiagnosticDetail(testConnector, SignalProtocolError, "http-5xx", "upstream said something")
	em.Availability(testConnector, "available")
	em.Capability(testConnector, CapabilityDiscovery, CapabilitySupported)
	em.Capability(testConnector, CapabilityDiscoveryExclusions, CapabilityUnsupported)
	em.ExclusionLimit(testConnector, 32)
	em.ExclusionCycle(testConnector, 34, 32)

	documented := map[string][]string{
		MetricDiagnosticsTotal:   {"connector", "reason", "signal"},
		MetricCapabilityInfo:     {"capability", "connector", "state"},
		MetricExclusionRequested: {"connector"},
		MetricExclusionAccepted:  {"connector"},
		MetricExclusionOverflow:  {"connector"},
		MetricExclusionPressure:  {"connector"},
		MetricExclusionLimit:     {"connector"},
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]bool{}
	for _, f := range families {
		name := f.GetName()
		if !strings.HasPrefix(name, "courier_connector_") {
			continue
		}
		got[name] = true
		want, documentedFamily := documented[name]
		if !documentedFamily {
			t.Errorf("unexpected family %s registered", name)
			continue
		}
		for _, m := range f.GetMetric() {
			keys := make([]string, 0, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				keys = append(keys, lp.GetName())
				if _, perr := strconv.ParseFloat(lp.GetValue(), 64); perr == nil {
					t.Errorf("family %s carries numeric value %q as a label value", name, lp.GetValue())
				}
			}
			slices.Sort(keys)
			if err := CheckLabelKeys(name, keys...); err != nil {
				t.Errorf("family %s: %v", name, err)
			}
			if !slices.Equal(keys, want) {
				t.Errorf("family %s labels = %v, want %v", name, keys, want)
			}
		}
	}
	for name := range documented {
		if !got[name] {
			t.Errorf("family %s not gathered after exercising the emitter", name)
		}
	}
}

// TestClosedReasonVocabulary is the closed-reason assertion (§11.1): for
// every registered (connector, signal) tuple, a declared reason passes
// through and an unrecognised reason maps to other.
func TestClosedReasonVocabulary(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	vocab := fullVocab()
	em.RegisterConnector(testConnector, vocab)

	for _, signal := range Signals {
		for _, reason := range vocab[signal] {
			em.Diagnostic(testConnector, signal, reason)
			got, ok := counterValue(t, reg, map[string]string{
				"connector": testConnector, "signal": signal, "reason": reason,
			})
			if !ok || got != 1 {
				t.Errorf("declared reason (%s, %s) = %v, ok=%v; want 1, true", signal, reason, got, ok)
			}
		}
		em.Diagnostic(testConnector, signal, "totally-unrecognised")
		got, ok := counterValue(t, reg, map[string]string{
			"connector": testConnector, "signal": signal, "reason": ReasonOther,
		})
		if !ok || got != 1 {
			t.Errorf("unrecognised reason for signal %s did not map to other (got %v, ok=%v)", signal, got, ok)
		}
	}

	em.Diagnostic("unregistered", SignalAck, "ok")
	if got, ok := counterValue(t, reg, map[string]string{
		"connector": "unregistered", "signal": SignalAck, "reason": ReasonOther,
	}); !ok || got != 1 {
		t.Errorf("unregistered connector reason did not map to other (got %v, ok=%v)", got, ok)
	}
}

// TestUnrecognisedSignalFoldsToOther checks that a signal outside the closed
// core vocabulary collapses onto the single catch-all instead of growing
// cardinality.
func TestUnrecognisedSignalFoldsToOther(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	em.RegisterConnector(testConnector, fullVocab())
	em.Diagnostic(testConnector, "made-up-signal", "made-up-reason")
	if _, ok := counterValue(t, reg, map[string]string{
		"connector": testConnector, "signal": ReasonOther, "reason": ReasonOther,
	}); !ok {
		t.Fatal("unknown signal/reason did not fold onto the other catch-all")
	}
}

// TestRegisterConnectorDuplicateAndPanic checks that duplicate registration
// raises no error and unknown signal names panic.
func TestRegisterConnectorDuplicateAndPanic(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	em.RegisterConnector(testConnector, map[string][]string{SignalAck: {"ok"}})
	em.RegisterConnector(testConnector, map[string][]string{SignalAck: {"ok", "missing"}})
	em.Diagnostic(testConnector, SignalAck, "missing")
	if got, ok := counterValue(t, reg, map[string]string{
		"connector": testConnector, "signal": SignalAck, "reason": "missing",
	}); !ok || got != 1 {
		t.Errorf("merged duplicate registration lost a declared reason (got %v, ok=%v)", got, ok)
	}

	defer func() {
		if recover() == nil {
			t.Error("RegisterConnector with unknown signal did not panic")
		}
	}()
	em.RegisterConnector(testConnector, map[string][]string{"made-up": {"x"}})
}

// forbiddenCorpus returns forbidden strings (§11.3): a token-like value, a
// repo name, a PR-number reference, and a head SHA. The repo name, PR
// reference, and SHA are registered on the redactor, which is the primary
// mechanism; the token shape is caught by the defensive pattern table.
func forbiddenCorpus() []string {
	return []string{
		"ghp_" + strings.Repeat("aB3xY9", 8),
		"octocat/skunkworks",
		"pull/4242",
		"8f2a4c9d1e0b7a6f5c3d2e1f0a9b8c7d6e5f4a3b",
	}
}

// TestDetailRedactedBeforeLogAndAbsentFromMetrics is the redaction guard
// (§11.3): forbidden strings fed as detail never reach the log line and never
// appear in any gathered metric label.
func TestDetailRedactedBeforeLogAndAbsentFromMetrics(t *testing.T) {
	em, sink, reg, buf := newTestEmitter(t, log.LevelDebug)
	for _, secret := range forbiddenCorpus() {
		sink.Emitter().Redactor().Register(secret)
	}
	detail := strings.Join(forbiddenCorpus(), "; ")
	em.RegisterConnector(testConnector, fullVocab())
	em.DiagnosticDetail(testConnector, SignalProtocolError, "http-5xx", detail)

	line := lastLine(t, buf)
	if raw, err := json.Marshal(line); err != nil {
		t.Fatalf("re-serialize line: %v", err)
	} else if written := string(raw); strings.Contains(written, "octocat/skunkworks") ||
		strings.Contains(written, "pull/4242") ||
		strings.Contains(written, "8f2a4c9d1e0b7a6f5c3d2e1f0a9b8c7d6e5f4a3b") ||
		strings.Contains(written, "ghp_") {
		t.Fatalf("forbidden string survived redaction in log line: %s", written)
	}
	logDetail, ok := line["detail"].(map[string]any)
	if !ok {
		t.Fatalf("log line has no detail object: %v", line)
	}
	if got, _ := logDetail["detail"].(string); !strings.Contains(got, log.RedactedPlaceholder) {
		t.Errorf("redacted detail = %q, want it to contain %q", got, log.RedactedPlaceholder)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				for _, secret := range forbiddenCorpus() {
					if strings.Contains(lp.GetValue(), secret) {
						t.Errorf("family %s label %s carries forbidden string", f.GetName(), lp.GetName())
					}
				}
			}
		}
	}
}

// TestDetailOnlyAtDebugLevel is the detail-channel assertion (§11.3): the
// free-form detail appears on the connector.diagnostic event only when debug
// logging is enabled, and only after redaction.
func TestDetailOnlyAtDebugLevel(t *testing.T) {
	emInfo, sinkInfo, _, bufInfo := newTestEmitter(t, log.LevelInfo)
	secret := "octocat/skunkworks ran pull/4242 at 8f2a4c9d1e0b7a6f5c3d2e1f0a9b8c7d6e5f4a3b"
	sinkInfo.Emitter().Redactor().Register("octocat/skunkworks")
	sinkInfo.Emitter().Redactor().Register("pull/4242")
	sinkInfo.Emitter().Redactor().Register("8f2a4c9d1e0b7a6f5c3d2e1f0a9b8c7d6e5f4a3b")
	emInfo.RegisterConnector(testConnector, fullVocab())
	emInfo.DiagnosticDetail(testConnector, SignalProtocolError, "http-5xx", secret)

	infoLine := lastLine(t, bufInfo)
	infoDetail, ok := infoLine["detail"].(map[string]any)
	if !ok {
		t.Fatalf("info log line has no detail object: %v", infoLine)
	}
	if _, present := infoDetail["detail"]; present {
		t.Errorf("free-form detail present at default level: %v", infoDetail)
	}
	if _, present := infoLine["detail"]; !present {
		t.Errorf("bounded diagnostic fields missing at default level: %v", infoLine)
	}

	emDebug, sinkDebug, _, bufDebug := newTestEmitter(t, log.LevelDebug)
	for _, s := range []string{"octocat/skunkworks", "pull/4242", "8f2a4c9d1e0b7a6f5c3d2e1f0a9b8c7d6e5f4a3b"} {
		sinkDebug.Emitter().Redactor().Register(s)
	}
	emDebug.RegisterConnector(testConnector, fullVocab())
	emDebug.DiagnosticDetail(testConnector, SignalProtocolError, "http-5xx", secret)

	debugLine := lastLine(t, bufDebug)
	debugDetail, ok := debugLine["detail"].(map[string]any)
	if !ok {
		t.Fatalf("debug log line has no detail object: %v", debugLine)
	}
	got, present := debugDetail["detail"].(string)
	if !present {
		t.Fatalf("free-form detail absent at debug level: %v", debugDetail)
	}
	if strings.Contains(got, "octocat/skunkworks") || strings.Contains(got, "pull/4242") ||
		strings.Contains(got, "8f2a4c9d1e0b7a6f5c3d2e1f0a9b8c7d6e5f4a3b") {
		t.Errorf("free-form detail reached debug log unredacted: %q", got)
	}
	if !strings.Contains(got, log.RedactedPlaceholder) {
		t.Errorf("debug detail = %q, want redaction via %q", got, log.RedactedPlaceholder)
	}
}

// TestCorrelationRidesLogHalfOnly is the no-correlation-label assertion
// (§11.4): the metric half's guard rejects correlation keys, while the log
// half may carry a run_id.
func TestCorrelationRidesLogHalfOnly(t *testing.T) {
	for _, key := range CorrelationKeys {
		err := RejectCorrelation(map[string]string{key: "value"})
		if !errors.Is(err, ErrCorrelationLabel) {
			t.Errorf("RejectCorrelation(%q) = %v, want ErrCorrelationLabel", key, err)
		}
	}
	if err := RejectCorrelation(map[string]string{"connector": testConnector, "signal": SignalAck}); err != nil {
		t.Errorf("RejectCorrelation on a clean label set = %v, want nil", err)
	}

	em, _, _, buf := newTestEmitter(t, log.LevelInfo)
	em.RegisterConnector(testConnector, fullVocab())
	em.WithRunID("coderun-abc").Diagnostic(testConnector, SignalAck, "ok")

	line := lastLine(t, buf)
	if line["run_id"] != "coderun-abc" {
		t.Errorf("log half run_id = %v, want coderun-abc", line["run_id"])
	}
	if line["event"] != EventConnectorDiagnostic {
		t.Errorf("log event type = %v, want %s", line["event"], EventConnectorDiagnostic)
	}

	buf.Reset()
	em.Diagnostic(testConnector, SignalAck, "ok")
	line = lastLine(t, buf)
	if runID, _ := line["run_id"].(string); strings.TrimSpace(runID) == "" {
		t.Errorf("unscoped diagnostic emitted no run_id: %v", line)
	}
}

// TestOneOverflowPerCycle is the one-overflow-per-cycle assertion (§11.2):
// an over-limit cycle counts the overflow diagnostic exactly once, the
// dropped count lives in the gauge value, pressure is requested/limit, and
// accepted < requested always leaves overflow positive.
func TestOneOverflowPerCycle(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	em.RegisterConnector(testConnector, fullVocab())
	conn := map[string]string{"connector": testConnector}

	em.ExclusionLimit(testConnector, 32)
	em.ExclusionCycle(testConnector, 34, 32)

	if got, ok := counterValue(t, reg, map[string]string{
		"connector": testConnector, "signal": SignalExclusion, "reason": ReasonOverflow,
	}); !ok || got != 1 {
		t.Fatalf("overflow counter after one over-limit cycle = %v, ok=%v; want 1, true", got, ok)
	}
	for name, want := range map[string]float64{
		MetricExclusionRequested: 34,
		MetricExclusionAccepted:  32,
		MetricExclusionOverflow:  2,
		MetricExclusionLimit:     32,
		MetricExclusionPressure:  34.0 / 32.0,
	} {
		if got, ok := gaugeValue(t, reg, name, conn); !ok || got != want {
			t.Errorf("%s = %v, ok=%v; want %v", name, got, ok, want)
		}
	}

	em.ExclusionCycle(testConnector, 0, 0)
	if got, _ := counterValue(t, reg, map[string]string{
		"connector": testConnector, "signal": SignalExclusion, "reason": ReasonOverflow,
	}); got != 1 {
		t.Errorf("overflow counter after a zero-request cycle = %v, want still 1", got)
	}
	for name, want := range map[string]float64{
		MetricExclusionRequested: 0,
		MetricExclusionOverflow:  0,
		MetricExclusionPressure:  0,
	} {
		if got, ok := gaugeValue(t, reg, name, conn); !ok || got != want {
			t.Errorf("%s after zero-request cycle = %v, ok=%v; want %v", name, got, ok, want)
		}
	}

	em.ExclusionCycle(testConnector, 40, 30)
	if got, _ := counterValue(t, reg, map[string]string{
		"connector": testConnector, "signal": SignalExclusion, "reason": ReasonOverflow,
	}); got != 2 {
		t.Errorf("overflow counter after second over-limit cycle = %v, want 2", got)
	}
	if got, _ := gaugeValue(t, reg, MetricExclusionOverflow, conn); got != 10 {
		t.Errorf("overflow gauge = %v, want 10", got)
	}

	requested, _ := gaugeValue(t, reg, MetricExclusionRequested, conn)
	accepted, _ := gaugeValue(t, reg, MetricExclusionAccepted, conn)
	overflow, _ := gaugeValue(t, reg, MetricExclusionOverflow, conn)
	if accepted < requested && overflow <= 0 {
		t.Errorf("accepted %v < requested %v but overflow = %v", accepted, requested, overflow)
	}
}

// TestCapabilityInfoOverwrite checks that switching a capability's state
// leaves exactly one series for the (connector, capability).
func TestCapabilityInfoOverwrite(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	em.Capability(testConnector, CapabilityDiscovery, CapabilitySupported)

	matchDiscovery := func(m *dto.Metric) bool {
		return labelValue(m, "connector") == testConnector && labelValue(m, "capability") == CapabilityDiscovery
	}
	if n := seriesCount(t, reg, MetricCapabilityInfo, matchDiscovery); n != 1 {
		t.Fatalf("series count after supported = %d, want 1", n)
	}
	m, _ := findSeries(t, reg, MetricCapabilityInfo, map[string]string{
		"connector": testConnector, "capability": CapabilityDiscovery, "state": CapabilitySupported,
	})
	if m == nil || m.GetGauge().GetValue() != 1 {
		t.Fatal("capability info gauge not 1 for supported state")
	}

	em.Capability(testConnector, CapabilityDiscovery, CapabilityUnsupported)
	if n := seriesCount(t, reg, MetricCapabilityInfo, matchDiscovery); n != 1 {
		t.Errorf("series count after supported→unsupported = %d, want exactly 1", n)
	}
	if _, ok := findSeries(t, reg, MetricCapabilityInfo, map[string]string{
		"connector": testConnector, "capability": CapabilityDiscovery, "state": CapabilitySupported,
	}); ok {
		t.Error("previous supported series still present after switch to unsupported")
	}
}

// TestCapabilityRejectsUnknownNames checks that an unknown capability is not
// advertised at all and an unknown state reports unknown.
func TestCapabilityRejectsUnknownNames(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	em.Capability(testConnector, "made-up", CapabilitySupported)
	if n := seriesCount(t, reg, MetricCapabilityInfo, func(*dto.Metric) bool { return true }); n != 0 {
		t.Errorf("unknown capability advertised with %d series, want 0", n)
	}
	em.Capability(testConnector, CapabilityDiscovery, "bogus-state")
	m, ok := findSeries(t, reg, MetricCapabilityInfo, map[string]string{
		"connector": testConnector, "capability": CapabilityDiscovery, "state": CapabilityUnknown,
	})
	if !ok || m.GetGauge().GetValue() != 1 {
		t.Error("unknown state did not report CapabilityUnknown")
	}
}

// TestNilSinkKeepsMetricHalf checks the emitter works with no log sink.
func TestNilSinkKeepsMetricHalf(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := NewEmitter(reg, nil)
	em.RegisterConnector(testConnector, fullVocab())
	em.DiagnosticDetail(testConnector, SignalProtocolError, "http-5xx", "dropped detail")
	if got, ok := counterValue(t, reg, map[string]string{
		"connector": testConnector, "signal": SignalProtocolError, "reason": "http-5xx",
	}); !ok || got != 1 {
		t.Errorf("counter with nil sink = %v, ok=%v; want 1, true", got, ok)
	}
}

// TestAvailabilityUsesAvailabilitySignal checks the convenience wrapper.
func TestAvailabilityUsesAvailabilitySignal(t *testing.T) {
	em, _, reg, buf := newTestEmitter(t, log.LevelInfo)
	em.RegisterConnector(testConnector, fullVocab())
	em.Availability(testConnector, "available")
	if got, ok := counterValue(t, reg, map[string]string{
		"connector": testConnector, "signal": SignalAvailability, "reason": "available",
	}); !ok || got != 1 {
		t.Errorf("availability counter = %v, ok=%v; want 1, true", got, ok)
	}
	line := lastLine(t, buf)
	if line["event"] != EventConnectorDiagnostic {
		t.Errorf("log event type = %v, want %s", line["event"], EventConnectorDiagnostic)
	}
	detail := line["detail"].(map[string]any)
	if detail["signal"] != SignalAvailability || detail["reason"] != "available" {
		t.Errorf("availability log detail = %v", detail)
	}
}

// TestExclusionLimitConflictFailsLoud checks that redeclaring a different
// exclusion limit for an already-declared connector fails loudly instead of
// silently clobbering the exported gauge, while redeclaring the same limit
// stays a no-op.
func TestExclusionLimitConflictFailsLoud(t *testing.T) {
	em, _, reg, _ := newTestEmitter(t, log.LevelInfo)
	em.ExclusionLimit(testConnector, 32)
	em.ExclusionLimit(testConnector, 32)
	if got, ok := gaugeValue(t, reg, MetricExclusionLimit, conn(testConnector)); !ok || got != 32 {
		t.Fatalf("limit gauge after identical redeclaration = %v, ok=%v; want 32", got, ok)
	}
	defer func() {
		if recover() == nil {
			t.Error("conflicting exclusion limit did not fail loudly")
		}
	}()
	em.ExclusionLimit(testConnector, 8)
}

// conn returns the label set identifying the test connector.
func conn(name string) map[string]string {
	return map[string]string{"connector": name}
}
