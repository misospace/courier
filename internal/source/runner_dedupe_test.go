package source

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// recordedLine is one captured Info line: the message and its key-value pairs.
type recordedLine struct {
	msg string
	kvs []any
}

func (l recordedLine) field(name string) (any, bool) {
	for i := 0; i+1 < len(l.kvs); i += 2 {
		if l.kvs[i] == name {
			return l.kvs[i+1], true
		}
	}
	return nil, false
}

// recordingSink captures the Info lines it receives, for assertions on log
// output. Enabled admits only lines at or below its configured verbosity, so
// a level-0 sink never sees a V(1) line: it mirrors the operator's default
// verbosity hiding the in-flight re-offer lines.
type recordingSink struct {
	mu        sync.Mutex
	verbosity int
	records   []recordedLine
}

func (s *recordingSink) Init(logr.RuntimeInfo) {}

func (s *recordingSink) Enabled(verbosity int) bool {
	return s.verbosity >= verbosity
}

func (s *recordingSink) Info(_ int, msg string, kvList ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kvs := make([]any, len(kvList))
	copy(kvs, kvList)
	s.records = append(s.records, recordedLine{msg: msg, kvs: kvs})
}

func (s *recordingSink) Error(_ error, _ string, _ ...any) {}

func (s *recordingSink) WithValues(_ ...any) logr.LogSink {
	return s
}

func (s *recordingSink) WithName(_ string) logr.LogSink {
	return s
}

// lines returns a copy of the recorded lines in order.
func (s *recordingSink) lines() []recordedLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedLine(nil), s.records...)
}

// dedupeRetainedRun builds the retained run the re-offer tests preload: owned
// by the test's source, keyed on the same work identity the adapter keeps
// offering, in the given phase with no pull request.
func dedupeRetainedRun(phase courierv1alpha1.Phase) *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: runName("test", "opaque-a"), Namespace: "courier"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     "test",
			WorkItemID: "opaque-a",
			Repo:       "acme/widgets",
			Ref:        1,
			Lane:       "local",
		},
		Status: courierv1alpha1.CoderRunStatus{Phase: phase},
	}
}

// dedupeFixture preloads the lane (concurrency 2) and a retained run in the
// given phase, and returns a runner on a fresh registry whose source keeps
// offering the retained run's work identity on every poll.
func dedupeFixture(t *testing.T, phase courierv1alpha1.Phase) (client.Client, *Runner, *prometheus.Registry) {
	t.Helper()
	lane := testLane()
	lane.Spec.Concurrency = 2
	kubeClient := newTestClient(t, lane, dedupeRetainedRun(phase))
	adapter := &testAdapter{items: []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}}
	reg := prometheus.NewRegistry()
	runner := newInstrumentedRunner(t, reg, kubeClient, adapter, RunnerConfig{Source: "test", LaneProfile: "local", Namespace: "courier"})
	return kubeClient, runner, reg
}

// TestRunnerPollReOfferCountsDuplicate pins the counted dedupe skip (#665):
// a source that keeps re-offering the identity of a retained run that already
// concluded absorbs every re-offer as a duplicate skip and never
// materializes a second run.
func TestRunnerPollReOfferCountsDuplicate(t *testing.T) {
	for _, phase := range []courierv1alpha1.Phase{courierv1alpha1.PhaseAwaitingReview, courierv1alpha1.PhaseDone} {
		t.Run(string(phase), func(t *testing.T) {
			kubeClient, runner, reg := dedupeFixture(t, phase)
			for i := 0; i < 3; i++ {
				if err := runner.Poll(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			var runs courierv1alpha1.CoderRunList
			if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 1 {
				t.Fatalf("coder runs = %d, want exactly 1: the retained run must stay the only run", len(runs.Items))
			}
			if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_skipped_total", map[string]string{"source": "test", "lane": "local", "reason": "duplicate"}); !ok || v != 3 {
				t.Fatalf("skipped[duplicate] = (%v, %v), want (3, true)", v, ok)
			}
			// The retained run is pre-created, not materialized by the
			// runner, so the materialized counter never fires.
			if v, ok := counterValueIfPresent(t, reg, "courier_source_items_materialized_total", map[string]string{"source": "test", "lane": "local"}); ok && v != 0 {
				t.Fatalf("materialized = %v, want absent or 0: the retained run was pre-created", v)
			}
		})
	}
}

// TestRunnerPollReOfferIsVisibleAtDefaultVerbosity pins the visible side of
// the dedupe skip (#665): a source that keeps re-offering work a retained run
// already concluded logs exactly one line at default verbosity — repeats of
// the same posture do not re-log, the metric carries the ongoing count.
func TestRunnerPollReOfferIsVisibleAtDefaultVerbosity(t *testing.T) {
	sink := &recordingSink{verbosity: 0}
	_, runner, _ := dedupeFixture(t, courierv1alpha1.PhaseAwaitingReview)
	ctx := crlog.IntoContext(context.Background(), logr.New(sink))
	for i := 0; i < 3; i++ {
		if err := runner.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	lines := sink.lines()
	if len(lines) != 1 {
		t.Fatalf("recorded lines = %d, want exactly 1: %#v", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0].msg, "work item was offered again") {
		t.Fatalf("line message = %q, want prefix %q", lines[0].msg, "work item was offered again")
	}
	if got, _ := lines[0].field("retainedRun"); got != runName("test", "opaque-a") {
		t.Fatalf("retainedRun = %v, want %q", got, runName("test", "opaque-a"))
	}
	if got, _ := lines[0].field("retainedPhase"); got != "AwaitingReview" {
		t.Fatalf("retainedPhase = %v, want AwaitingReview", got)
	}
	if got, present := lines[0].field("operatorAction"); !present || got == nil || got == "" {
		t.Fatalf("operatorAction = (%v, %v), want the action that ends the repeats named on the concluded-run line", got, present)
	}
}

// TestRunnerPollReOfferLogsOncePerPassAndReappearance pins both bounds of the
// posture contract (#665): one pass offering the same retained identity twice
// counts both absorbs but logs one line, and an identity the source stops
// offering forgets its posture, so the next offer re-logs it.
func TestRunnerPollReOfferLogsOncePerPassAndReappearance(t *testing.T) {
	sink := &recordingSink{verbosity: 0}
	kubeClient, runner, reg := dedupeFixture(t, courierv1alpha1.PhaseAwaitingReview)
	adapter := runner.Adapter.(*testAdapter)
	adapter.items = []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}, {ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}
	ctx := crlog.IntoContext(context.Background(), logr.New(sink))
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if lines := sink.lines(); len(lines) != 1 {
		t.Fatalf("lines after doubled offer = %d, want 1: %#v", len(lines), lines)
	}
	if v, ok := counterValueIfPresent(t, reg, "courier_source_discovery_skipped_total", map[string]string{"source": "test", "lane": "local", "reason": "duplicate"}); !ok || v != 2 {
		t.Fatalf("skipped[duplicate] = (%v, %v), want (2, true): every absorbed offer is counted", v, ok)
	}
	// The source stops offering the identity: the pass replaces the posture
	// map, so the forgotten posture re-logs when the offer returns.
	adapter.items = nil
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	adapter.items = []WorkItem{{ID: "opaque-a", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 1}}
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if lines := sink.lines(); len(lines) != 2 {
		t.Fatalf("lines after reappearance = %d, want 2: %#v", len(lines), lines)
	}
	var runs courierv1alpha1.CoderRunList
	if err := kubeClient.List(context.Background(), &runs, client.InNamespace("courier")); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("coder runs = %d, want exactly 1: absorbed offers never materialize a run", len(runs.Items))
	}
}

// TestRunnerPollInFlightReOfferStaysQuietAtDefaultVerbosity pins the other
// side of the verbosity split (#665): a re-offer absorbed by a run still in
// flight is normal claim traffic, so it logs at V(1) and stays invisible at
// default verbosity.
func TestRunnerPollInFlightReOfferStaysQuietAtDefaultVerbosity(t *testing.T) {
	t.Run("default verbosity stays quiet", func(t *testing.T) {
		sink := &recordingSink{verbosity: 0}
		_, runner, _ := dedupeFixture(t, courierv1alpha1.PhaseRunning)
		ctx := crlog.IntoContext(context.Background(), logr.New(sink))
		for i := 0; i < 3; i++ {
			if err := runner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if lines := sink.lines(); len(lines) != 0 {
			t.Fatalf("recorded lines = %d, want 0 at default verbosity: %#v", len(lines), lines)
		}
	})
	t.Run("verbosity 1 records", func(t *testing.T) {
		sink := &recordingSink{verbosity: 1}
		_, runner, _ := dedupeFixture(t, courierv1alpha1.PhaseRunning)
		ctx := crlog.IntoContext(context.Background(), logr.New(sink))
		for i := 0; i < 3; i++ {
			if err := runner.Poll(ctx); err != nil {
				t.Fatal(err)
			}
		}
		lines := sink.lines()
		if len(lines) != 1 {
			t.Fatalf("recorded lines = %d, want exactly 1: %#v", len(lines), lines)
		}
		if got, _ := lines[0].field("retainedPhase"); got != "Running" {
			t.Fatalf("retainedPhase = %v, want Running", got)
		}
		if _, present := lines[0].field("operatorAction"); present {
			t.Fatal("operatorAction present on the V(1) in-flight line, want absent")
		}
	})
}

// TestRunnerPollReOfferRelogsWhenOwnerPostureChanges pins the once-per-posture
// contract (#665): the same identity re-logs when the retained run's phase
// changes, so the operator sees the transition instead of only the first
// repeat.
func TestRunnerPollReOfferRelogsWhenOwnerPostureChanges(t *testing.T) {
	sink := &recordingSink{verbosity: 0}
	kubeClient, runner, _ := dedupeFixture(t, courierv1alpha1.PhaseAwaitingReview)
	ctx := crlog.IntoContext(context.Background(), logr.New(sink))
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	lines := sink.lines()
	if len(lines) != 1 {
		t.Fatalf("lines after first poll = %d, want 1: %#v", len(lines), lines)
	}
	if got, _ := lines[0].field("retainedPhase"); got != "AwaitingReview" {
		t.Fatalf("first line retainedPhase = %v, want AwaitingReview", got)
	}
	// The fake client has no status subresource here, so a plain Update
	// changes the phase in place.
	fresh := &courierv1alpha1.CoderRun{}
	key := client.ObjectKeyFromObject(dedupeRetainedRun(courierv1alpha1.PhaseAwaitingReview))
	if err := kubeClient.Get(ctx, key, fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Status.Phase = courierv1alpha1.PhaseDone
	if err := kubeClient.Update(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if err := runner.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	lines = sink.lines()
	if len(lines) != 2 {
		t.Fatalf("lines after phase change = %d, want 2: %#v", len(lines), lines)
	}
	if got, _ := lines[1].field("retainedPhase"); got != "Done" {
		t.Fatalf("second line retainedPhase = %v, want Done", got)
	}
}
